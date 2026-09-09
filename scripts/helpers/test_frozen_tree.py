"""Per-run frozen source trees; offline, against throwaway git repositories."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import frozen_tree as subject
import vz04_source_tree as source_tree

REPO = Path(__file__).resolve().parents[2]


def git(root, *args):
    completed = subprocess.run(["git", "-C", str(root), *args], stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if completed.returncode != 0:
        raise AssertionError("git " + " ".join(args) + ": " + completed.stderr.decode())
    return completed.stdout.decode().rstrip("\n")


class Fixture(unittest.TestCase):
    """A tiny real repository: `git worktree add` needs real git objects."""

    def setUp(self):
        subject._records.clear()
        self.tmp = tempfile.TemporaryDirectory(prefix="vz-frozen-")
        self.root = Path(self.tmp.name).resolve()
        git(self.root, "init", "--quiet", "-b", "main")
        git(self.root, "config", "user.name", "vz test")
        git(self.root, "config", "user.email", "test@invalid")
        git(self.root, "config", "commit.gpgsign", "false")
        (self.root / "scripts" / "helpers").mkdir(parents=True)
        (self.root / "crates").mkdir()
        (self.root / "crates" / "Cargo.lock").write_text("# lock\n")
        (self.root / "scripts" / "helpers" / "entry.py").write_text(
            "import json, sys\n"
            "from pathlib import Path\n"
            "root = Path(__file__).resolve().parents[2]\n"
            "print(json.dumps({'cwd': __import__('os').getcwd(), 'root': str(root),\n"
            "                  'sys_path0': sys.path[0], 'argv': sys.argv[1:],\n"
            "                  'body': (root / 'body.txt').read_text()}))\n")
        (self.root / "body.txt").write_text("committed\n")
        probe = self.root / "probe.sh"
        probe.write_text("#!/bin/sh\nexit 0\n")
        probe.chmod(0o755)
        git(self.root, "add", "-A")
        git(self.root, "commit", "--quiet", "-m", "base")
        self.created = []

    def tearDown(self):
        for tree in self.created:
            tree.release()
        self.tmp.cleanup()
        subject._records.clear()

    def freeze(self):
        tree = subject.freeze(self.root)
        self.created.append(tree)
        return tree


class FreezeTests(Fixture):
    def test_frozen_tree_reproduces_a_clean_checkout(self):
        tree = self.freeze()
        self.assertTrue(tree.info["frozen"])
        self.assertEqual(tree.info["commit"], git(self.root, "rev-parse", "HEAD"))
        self.assertEqual(tree.info["git_tree"], git(self.root, "rev-parse", "HEAD^{tree}"))
        self.assertEqual(tree.info["tree_sha256"], source_tree.tree_digest(self.root)[0])
        self.assertEqual(tree.info["tracked_change_count"], 0)
        self.assertEqual((tree.path / "body.txt").read_text(), "committed\n")

    def test_frozen_tree_lives_under_private_tmp_within_its_bound(self):
        tree = self.freeze()
        self.assertTrue(str(tree.path).startswith("/private/tmp/" + subject.FROZEN_PREFIX))
        self.assertLessEqual(len(str(tree.path).encode()), subject.FROZEN_ROOT_LIMIT)
        # The Docker endpoint name a Machine is given still fits beside it, even
        # though no lane derives an AF_UNIX path from the source tree.
        endpoint = len(str(tree.path).encode()) + 1 + 45
        self.assertLessEqual(endpoint, 103)

    def test_frozen_tree_carries_uncommitted_tracked_edits(self):
        (self.root / "body.txt").write_text("edited\n")
        tree = self.freeze()
        self.assertEqual((tree.path / "body.txt").read_text(), "edited\n")
        self.assertEqual(tree.info["tracked_changes"], ["M body.txt"])
        self.assertEqual(tree.info["tree_sha256"], source_tree.tree_digest(self.root)[0])

    def test_frozen_tree_preserves_the_executable_bit(self):
        tree = self.freeze()
        self.assertTrue(os.access(tree.path / "probe.sh", os.X_OK))
        (self.root / "probe.sh").write_text("#!/bin/sh\nexit 1\n")
        second = self.freeze()
        self.assertTrue(os.access(second.path / "probe.sh", os.X_OK))

    def test_frozen_tree_omits_untracked_checkout_content(self):
        (self.root / "sibling").mkdir()
        (self.root / "sibling" / "huge.bin").write_bytes(b"x" * 1024)
        tree = self.freeze()
        self.assertFalse((tree.path / "sibling").exists())

    def test_a_checkout_that_cannot_be_digested_is_refused_before_anything_is_created(self):
        (self.root / "body.txt").unlink()
        before = sorted(p.name for p in Path("/private/tmp").glob(subject.FROZEN_PREFIX + "*"))
        with self.assertRaises(subject.FreezeError) as caught:
            subject.freeze(self.root)
        self.assertIn("could not name its tree", str(caught.exception))
        self.assertEqual(before, sorted(p.name for p in Path("/private/tmp").glob(subject.FROZEN_PREFIX + "*")))

    def test_freezing_a_directory_that_is_not_a_work_tree_is_refused(self):
        plain = Path(tempfile.mkdtemp(prefix="vz-frozen-plain-"))
        try:
            with self.assertRaises(subject.FreezeError):
                subject.freeze(plain)
        finally:
            shutil.rmtree(plain, ignore_errors=True)

    def test_release_removes_the_tree_and_its_registration(self):
        tree = subject.freeze(self.root)
        path, holder = tree.path, tree.holder
        self.assertIn(str(path), git(self.root, "worktree", "list"))
        tree.release()
        self.assertFalse(path.exists())
        self.assertFalse(holder.exists())
        self.assertNotIn(str(path), git(self.root, "worktree", "list"))

    def test_release_survives_a_tree_removed_underneath_it(self):
        tree = subject.freeze(self.root)
        shutil.rmtree(tree.path)
        tree.release()
        self.assertFalse(tree.holder.exists())
        self.assertNotIn(str(tree.path), git(self.root, "worktree", "list"))

    def test_freezing_the_checkout_never_changes_it(self):
        before = source_tree.tree_digest(self.root)
        status = git(self.root, "status", "--porcelain=v1", "--untracked-files=all")
        tree = self.freeze()
        self.assertEqual(source_tree.tree_digest(self.root), before)
        self.assertEqual(git(self.root, "status", "--porcelain=v1", "--untracked-files=all"), status)
        self.assertTrue(tree.path.is_dir())

    def test_a_frozen_tree_survives_the_checkout_moving_under_it(self):
        tree = self.freeze()
        frozen_digest = source_tree.tree_digest(tree.path)[0]
        (self.root / "body.txt").write_text("merged while the run was live\n")
        (self.root / "scripts" / "helpers" / "entry.py").write_text("print('replaced')\n")
        self.assertNotEqual(source_tree.tree_digest(self.root)[0], frozen_digest)
        self.assertEqual(source_tree.tree_digest(tree.path)[0], frozen_digest)
        self.assertEqual((tree.path / "body.txt").read_text(), "committed\n")


class RecordTests(Fixture):
    def test_record_inside_a_frozen_tree_is_the_record_the_freeze_wrote(self):
        tree = self.freeze()
        self.assertEqual(subject.record(tree.path), tree.info)
        self.assertTrue(subject.record(tree.path)["frozen"])

    def test_record_of_a_live_checkout_says_it_was_not_frozen(self):
        row = subject.record(self.root)
        self.assertFalse(row["frozen"])
        self.assertEqual(row["root"], row["live_root"])
        self.assertEqual(row["commit"], git(self.root, "rev-parse", "HEAD"))
        self.assertEqual(row["tree_sha256"], source_tree.tree_digest(self.root)[0])

    def test_record_of_an_undescribable_tree_is_sentinel_digests_not_a_raise(self):
        plain = Path(tempfile.mkdtemp(prefix="vz-frozen-plain-"))
        try:
            row = subject.record(plain)
        finally:
            shutil.rmtree(plain, ignore_errors=True)
        self.assertEqual(row, subject.unknown(plain))
        self.assertEqual(row["commit"], subject.UNKNOWN_COMMIT)
        self.assertEqual(row["tree_sha256"], subject.UNKNOWN_DIGEST)
        self.assertFalse(row["frozen"])

    def test_record_bounds_the_change_list_but_not_the_count(self):
        for index in range(subject.CHANGE_LIMIT + 5):
            path = self.root / f"file-{index:03}.txt"
            path.write_text("new\n")
        git(self.root, "add", "-A")
        git(self.root, "commit", "--quiet", "-m", "many")
        for index in range(subject.CHANGE_LIMIT + 5):
            (self.root / f"file-{index:03}.txt").write_text("changed\n")
        row = subject.record(self.root)
        self.assertEqual(row["tracked_change_count"], subject.CHANGE_LIMIT + 5)
        self.assertEqual(len(row["tracked_changes"]), subject.CHANGE_LIMIT)

    def test_record_is_cached_per_root(self):
        first = subject.record(self.root)
        (self.root / "body.txt").write_text("changed after the first read\n")
        self.assertEqual(subject.record(self.root), first)

    def test_record_returns_a_copy_callers_cannot_corrupt(self):
        row = subject.record(self.root)
        row["commit"] = "z" * 40
        self.assertNotEqual(subject.record(self.root)["commit"], "z" * 40)


class BootstrapTests(Fixture):
    def run_bootstrap(self, *args, cwd=None):
        shutil.copy2(REPO / "scripts/helpers/frozen_tree.py", self.root / "scripts/helpers/frozen_tree.py")
        shutil.copy2(REPO / "scripts/helpers/vz04_source_tree.py", self.root / "scripts/helpers/vz04_source_tree.py")
        git(self.root, "add", "-A")
        git(self.root, "commit", "--quiet", "-m", "bootstrap")
        return subprocess.run(
            ["/usr/bin/python3", "-B", str(self.root / "scripts/helpers/frozen_tree.py"),
             "--entry", "scripts/helpers/entry.py", "--", *args],
            cwd=str(cwd or self.root), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, check=False)

    def test_bootstrap_runs_the_entry_point_from_the_frozen_tree(self):
        completed = self.run_bootstrap("--suite", "all")
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        payload = json.loads(completed.stdout.decode().splitlines()[-1])
        self.assertTrue(payload["root"].startswith("/private/tmp/" + subject.FROZEN_PREFIX))
        self.assertEqual(payload["cwd"], payload["root"])
        self.assertEqual(payload["sys_path0"], payload["root"] + "/scripts/helpers")
        self.assertEqual(payload["argv"], ["--suite", "all"])
        self.assertNotEqual(payload["root"], str(self.root))

    def test_bootstrap_removes_the_frozen_tree_when_the_run_ends(self):
        completed = self.run_bootstrap()
        payload = json.loads(completed.stdout.decode().splitlines()[-1])
        self.assertFalse(Path(payload["root"]).exists())
        self.assertNotIn(payload["root"], git(self.root, "worktree", "list"))

    def test_bootstrap_rejects_argv_it_does_not_understand(self):
        shutil.copy2(REPO / "scripts/helpers/frozen_tree.py", self.root / "scripts/helpers/frozen_tree.py")
        shutil.copy2(REPO / "scripts/helpers/vz04_source_tree.py", self.root / "scripts/helpers/vz04_source_tree.py")
        completed = subprocess.run(
            ["/usr/bin/python3", "-B", str(self.root / "scripts/helpers/frozen_tree.py"), "--suite", "all"],
            cwd=str(self.root), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("usage:", completed.stderr.decode())


PINNING_PROBE = r"""
import json, sys
sys.path.insert(0, sys.argv[1] + "/scripts/helpers")
import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_limits_machine as limits

paths = limits.required_source_paths()
pins = {name: startup.digest(name) for name in paths}
limits.verify_sources(pins)
target = sys.argv[1] + "/scripts/helpers/linux_docker_limits_machine.py"
with open(target, "ab") as stream:
    stream.write(b"\n# mutated inside the run's own frozen tree\n")
try:
    limits.verify_sources(pins)
    rejected = None
except driver.Rejected as error:
    rejected = str(error)
print(json.dumps({"paths": paths, "rejected": rejected}))
"""


class PinningTests(Fixture):
    """The pins bind the frozen tree, and mutating it still aborts the run."""

    def test_source_pins_bind_the_frozen_tree_and_still_abort_on_its_mutation(self):
        tree = self.freeze_real_repo()
        completed = subprocess.run([sys.executable, "-B", "-c", PINNING_PROBE, str(tree.path)],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, check=False)
        if completed.returncode != 0 and b"ModuleNotFoundError" in completed.stderr:
            self.skipTest("suite modules need the pinned uv dependencies: " + completed.stderr.decode()[-200:])
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        payload = json.loads(completed.stdout.decode().splitlines()[-1])
        # Every pinned path is inside the frozen tree, so no edit to the working
        # checkout is visible to the run at all.
        for path in payload["paths"]:
            self.assertTrue(path.startswith(str(tree.path) + "/"), path)
            self.assertFalse(path.startswith(str(REPO) + "/"), path)
        # ... and the check that lost a 43-minute run still fires, against the
        # one tree it is now allowed to fire against.
        self.assertIsNotNone(payload["rejected"], "mutating the frozen tree must abort the run")
        self.assertIn("limits source changed", payload["rejected"])

    def freeze_real_repo(self):
        tree = subject.freeze(REPO)
        self.created.append(tree)
        return tree


class EntryPointTests(unittest.TestCase):
    def test_the_linux_docker_entry_point_runs_the_harness_through_the_freeze(self):
        text = (REPO / "scripts/run-linux-docker-e2e.sh").read_text()
        self.assertIn("helpers/frozen_tree.py", text)
        self.assertIn("--entry scripts/helpers/linux_docker_e2e.py --", text)
        # No second mode: the harness is never invoked directly by the wrapper.
        self.assertNotIn('python -B "$script_dir/helpers/linux_docker_e2e.py"', text)

    def test_the_lane_result_schema_requires_the_tree_a_run_read(self):
        schema = json.loads((REPO / "schemas/vz-0.4-lane-result.schema.json").read_text())
        self.assertIn("source_tree", schema["required"])
        self.assertEqual(schema["properties"]["source_tree"]["additionalProperties"], False)
        self.assertEqual(sorted(schema["properties"]["source_tree"]["properties"]), sorted(subject.unknown("/x")))


if __name__ == "__main__":
    unittest.main()
