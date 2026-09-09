#!/usr/bin/env python3
"""Per-run frozen source tree for hardware lanes, and the identity it records.

WHY. Every suite module pins its own source paths (`required_source_paths` /
`verify_sources`) and re-verifies them DURING the run, so a lane result can
never be produced from a tree that changed underneath it. That check is
correct and is not weakened here. Its cost was that the working checkout had to
stand still for the 50-55 minutes a composed run takes: merging an agent branch
mid-run aborted a 43-minute run with `limits source changed:
scripts/helpers/linux_docker_e2e.py`.

WHAT. The lane entry point no longer runs the helpers in the working checkout.
It freezes the checkout into a private git worktree, re-executes itself from
there, and removes the frozen tree when the run ends. The pins then bind the
frozen tree: the checkout is free for merges, and mutating the run's own frozen
tree still aborts it exactly as before.

FIDELITY. The freeze is `git worktree add --detach` at HEAD plus an overlay of
every tracked file whose working-tree bytes differ from HEAD (and the removal of
every tracked file deleted in the checkout). So the frozen tree carries the
uncommitted edits that were present at freeze time, and
`vz04_source_tree.tree_digest` over it equals the digest over the checkout at
that instant -- asserted in `freeze`, not assumed. Untracked files are NOT
carried: no lane input is untracked, and the checkout's untracked set includes
whole sibling worktrees.

SOCKET BUDGET. A Machine's Docker endpoint name is `vzr1-ot-<32hex>.sock`, 45 of
macOS's 103 bindable `sun_path` bytes, and a too-long root is total failure, not
a warning (vz-tzc). No lane derives an AF_UNIX path from the source tree:
`installed_developer_startup.Harness.__init__` mkdtemps its runtime root under
`/private/tmp`, and `developer_environment_recorder.socket_root_for` hashes the
state root into `/private/tmp/vzt-<12hex>`. The frozen tree therefore cannot
spend the budget. It is placed under `/private/tmp` anyway and bounded by
`FROZEN_ROOT_LIMIT` so that stays true if a lane ever does derive one from it.

IDENTITY. `record()` returns the tree a run actually executed against --
`commit`, `git_tree`, `tree_sha256` in exactly the sense
`scripts/helpers/vz04_source_tree.py` defines them for the release manifest --
so a lane result names its tree instead of implying the checkout.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import vz04_source_tree as source_tree

# The frozen tree announces itself by a file only `freeze` writes, so the
# re-execution is re-entrant by structure. There is no flag, and no environment
# variable, that runs a lane from the live checkout instead.
MARKER = ".vz-frozen-tree.json"
FROZEN_BASE = Path("/private/tmp")
FROZEN_PREFIX = "vzfrz-"
FROZEN_LEAF = "repo"
# See SOCKET BUDGET above. 103 bindable bytes minus the 45-byte endpoint name
# leaves 58 for a root; a frozen root is bounded well inside that so it can
# never be the thing that overruns it.
FROZEN_ROOT_LIMIT = 48
UNKNOWN_COMMIT = "0" * 40
UNKNOWN_DIGEST = "0" * 64
# Bounded so one badly-timed `git status` cannot put a megabyte of paths into
# every lane result; the digest, not the list, is the identity.
CHANGE_LIMIT = 64


class FreezeError(Exception):
    """The source tree could not be frozen, described, or released."""


def _git(root: Path, *args: str, binary: bool = False):
    completed = subprocess.run(["git", "-C", str(root), *args], stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600, check=False)
    if completed.returncode != 0:
        raise FreezeError("git " + " ".join(args) + " failed: "
                          + completed.stderr.decode("utf-8", "replace").strip())
    return completed.stdout if binary else completed.stdout.decode("utf-8", "replace").rstrip("\n")


def _tracked_changes(root: Path) -> list:
    """`[(status, path)]` for every tracked path whose working bytes differ from HEAD.

    `--no-renames` so each record is one status and one path: the overlay copies
    or deletes paths, and a rename is exactly a delete plus an add.
    """
    raw = _git(root, "diff", "--name-status", "-z", "--no-renames", "HEAD", binary=True)
    fields = [item for item in raw.split(b"\0") if item]
    if len(fields) % 2:
        raise FreezeError("unparseable git diff --name-status output")
    rows = []
    for index in range(0, len(fields), 2):
        status = fields[index].decode("ascii", "replace")
        path = fields[index + 1].decode("utf-8", "surrogateescape")
        rows.append((status, path))
    return sorted(rows, key=lambda row: row[1])


def _stage(frozen: Path, changes: list) -> None:
    """Record the overlay in the frozen worktree's own index.

    `vz04_source_tree` digests the tracked set, which it reads from the index.
    A file the checkout has staged but not committed is tracked there and absent
    from a worktree created at HEAD, so without this the frozen tree would be
    missing exactly the source an agent had just added.
    """
    if not changes:
        return
    payload = b"".join(path.encode("utf-8", "surrogateescape") + b"\0" for _status, path in changes)
    completed = subprocess.run(["git", "-C", str(frozen), "add", "-A", "--force",
                                "--pathspec-from-file=-", "--pathspec-file-nul"],
                               input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=600, check=False)
    if completed.returncode != 0:
        raise FreezeError("staging the frozen overlay failed: "
                          + completed.stderr.decode("utf-8", "replace").strip())


def _overlay(live_root: Path, frozen: Path, changes: list) -> None:
    """Reproduce the checkout's uncommitted tracked state inside the frozen worktree."""
    for status, relative in changes:
        target = frozen / relative
        if status.startswith("D"):
            if target.is_symlink() or target.exists():
                target.unlink()
            continue
        origin = live_root / relative
        if origin.is_symlink():
            if target.is_symlink() or target.exists():
                target.unlink()
            target.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(os.readlink(origin), target)
            continue
        if not origin.is_file():
            raise FreezeError("tracked change is neither a regular file nor a symlink: " + relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_symlink():
            target.unlink()
        # copy2 carries the mode bits, so a fixture's executable bit survives.
        shutil.copy2(origin, target)


def _describe(root: Path, *, frozen: bool, live_root: Path, changes: list) -> dict:
    digest, count = source_tree.tree_digest(root)
    return {"frozen": frozen, "root": str(root), "live_root": str(live_root),
            "commit": _git(root, "rev-parse", "HEAD"),
            "git_tree": _git(root, "rev-parse", "HEAD^{tree}"),
            "tree_sha256": digest, "tracked_file_count": count,
            "tracked_changes": [status + " " + path for status, path in changes[:CHANGE_LIMIT]],
            "tracked_change_count": len(changes)}


def unknown(root) -> dict:
    """The record for a tree whose git identity could not be read.

    Digest sentinels rather than a raised error, the way `lane_result.base`
    already records an unreadable release directory: the lane result stays
    schema-valid and says plainly that it cannot name its tree.
    """
    return {"frozen": False, "root": str(root), "live_root": str(root), "commit": UNKNOWN_COMMIT,
            "git_tree": UNKNOWN_COMMIT, "tree_sha256": UNKNOWN_DIGEST, "tracked_file_count": 0,
            "tracked_changes": [], "tracked_change_count": 0}


_records: dict = {}


def record(root) -> dict:
    """The identity of the tree `root` is, cached per root.

    Inside a frozen tree this is the record `freeze` wrote, so the run reports
    the tree it was given rather than re-deriving one. Anywhere else it is a
    description of that tree with `frozen: false`, which is what a lane not yet
    cut over to freezing honestly ran against.
    """
    root = Path(root)
    key = str(root)
    if key in _records:
        return dict(_records[key])
    marker = root / MARKER
    value = None
    if marker.is_file() and not marker.is_symlink():
        try:
            value = json.loads(marker.read_bytes().decode("utf-8"))
        except (OSError, ValueError):
            value = None
    if not isinstance(value, dict):
        try:
            value = _describe(root, frozen=False, live_root=root, changes=_tracked_changes(root))
        except (FreezeError, source_tree.SourceTreeError, OSError):
            value = unknown(root)
    _records[key] = value
    return dict(value)


class FrozenTree:
    """A private git worktree holding the checkout's bytes for one run."""

    def __init__(self, live_root: Path, holder: Path, path: Path, info: dict):
        self.live_root = live_root
        self.holder = holder
        self.path = path
        self.info = info

    def release(self) -> None:
        """Remove the worktree and its holder.

        `worktree remove` drops this run's registration and nothing else. Only
        when it fails is the directory removed by hand and `prune` used to clear
        the registration it left behind -- a repo this busy has many other
        worktrees registered, and pruning is not run speculatively over them.
        """
        try:
            _git(self.live_root, "worktree", "remove", "--force", str(self.path))
        except FreezeError:
            shutil.rmtree(self.path, ignore_errors=True)
            try:
                _git(self.live_root, "worktree", "prune")
            except FreezeError:
                pass
        shutil.rmtree(self.holder, ignore_errors=True)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.release()
        return False


def freeze(live_root) -> FrozenTree:
    """Freeze `live_root` into a private worktree under `/private/tmp`.

    Fails rather than proceeding if the frozen tree's tracked bytes differ from
    the checkout's: a freeze that silently ran different source than the operator
    is holding is worse than no freeze at all.
    """
    live_root = Path(live_root).resolve()
    if not (live_root / ".git").exists():
        raise FreezeError("not a git work tree: " + str(live_root))
    # The digest the frozen tree has to reproduce, taken before anything is
    # created. A checkout that cannot be digested cannot have its tree named in
    # a lane result, so it is refused here with the reason rather than deep
    # inside the freeze.
    try:
        expected, _ = source_tree.tree_digest(live_root)
    except source_tree.SourceTreeError as error:
        raise FreezeError("checkout cannot be digested, so a run against it could not name its tree: "
                          + str(error)) from None
    commit = _git(live_root, "rev-parse", "HEAD")
    holder = Path(tempfile.mkdtemp(prefix=FROZEN_PREFIX, dir=str(FROZEN_BASE)))
    path = holder / FROZEN_LEAF
    if len(str(path).encode()) > FROZEN_ROOT_LIMIT:
        shutil.rmtree(holder, ignore_errors=True)
        raise FreezeError("frozen root over its %d-byte bound: %s" % (FROZEN_ROOT_LIMIT, path))
    try:
        _git(live_root, "worktree", "add", "--detach", "--quiet", str(path), commit)
        changes = _tracked_changes(live_root)
        _overlay(live_root, path, changes)
        _stage(path, changes)
        info = _describe(path, frozen=True, live_root=live_root, changes=changes)
        if info["tree_sha256"] != expected:
            raise FreezeError("frozen tree does not reproduce the checkout: %s != %s"
                              % (info["tree_sha256"], expected))
        (path / MARKER).write_text(json.dumps(info, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except BaseException:
        FrozenTree(live_root, holder, path, {}).release()
        raise
    return FrozenTree(live_root, holder, path, info)


def _usage() -> str:
    return "usage: frozen_tree.py --entry <repo-relative script> -- <argv...>"


def main(argv) -> int:
    """Run `--entry` from a frozen copy of this tree, then remove the copy.

    Re-entrant by the marker file: invoked from inside a frozen tree it runs the
    entry point directly, so the frozen re-execution does not freeze again.
    """
    argv = list(argv)
    if len(argv) < 3 or argv[0] != "--entry" or argv[2] != "--":
        print(_usage(), file=sys.stderr)
        return 2
    entry, rest = argv[1], argv[3:]
    here = Path(__file__).resolve().parents[2]
    if (here / MARKER).is_file():
        os.execv(sys.executable, [sys.executable, "-B", str(here / entry), *rest])
    try:
        tree = freeze(here)
    except (FreezeError, source_tree.SourceTreeError) as error:
        print("error: could not freeze the source tree: " + str(error), file=sys.stderr)
        return 2
    print("==> frozen tree %s commit %s tree_sha256 %s (%d tracked change(s))"
          % (tree.path, tree.info["commit"][:12], tree.info["tree_sha256"][:12], tree.info["tracked_change_count"]),
          flush=True)
    try:
        completed = subprocess.run([sys.executable, "-B", str(tree.path / entry), *rest],
                                   cwd=str(tree.path), stdin=subprocess.DEVNULL, check=False)
        return completed.returncode
    finally:
        tree.release()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
