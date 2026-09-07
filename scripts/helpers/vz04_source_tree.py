"""Canonical source-tree digest and fresh-checkout assertions for the vz 0.4 release candidate.

Python 3.9 standard library only. The release-candidate builder
(`scripts/build-vz-0.4-release-candidate.sh`) calls this module before and after
building so the manifest records what was actually compiled, not merely which
commit was checked out.

Definitions (normative for `release-manifest.json#/source`):

* `commit`: `git rev-parse HEAD`.
* `git_tree`: `git rev-parse HEAD^{tree}`.
* `tree_sha256`: SHA-256 of `canonical_json([[path, gitmode, sha256(content)], ...])`
  over `git ls-files -z -s` sorted by path bytes, where `content` is the working
  tree bytes of the tracked file (or the link target for mode 120000 entries). On
  a clean checkout this equals the digest of the committed blobs; on a
  development checkout it covers the modified bytes that were built.
* `canonical_json(value)`: `json.dumps(value, sort_keys=True, separators=(",", ":"),
  ensure_ascii=True)` encoded as UTF-8 followed by a single LF. The same function
  is exposed as the `canonical-sha256` subcommand so Bash callers reuse one
  canonicalisation for `normalized_content_sha256` and `signed_content_sha256`.

Cleanliness assertions (`describe(..., allow_unclean=False)`):

* `git status --porcelain=v1 --untracked-files=all -z` is empty.
* Every `git submodule status --recursive` entry is checked out at its recorded
  commit (no `+`, `-` or `U` prefix). A repository without submodules is clean.
* `crates/Cargo.lock` is tracked and unchanged. This is enforced even when
  `allow_unclean=True`, because `cargo build --locked` is only meaningful against
  the committed lockfile.

`allow_unclean=True` is DEVELOPMENT ONLY: it records `clean: false` together with
the tracked changes and untracked paths so a certification-grade validator can
reject the manifest.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

LOCKFILE = "crates/Cargo.lock"
MAX_TRACKED_FILE_BYTES = 512 * 1024 * 1024


class SourceTreeError(Exception):
    """A fresh-checkout assertion failed or the tree could not be described."""


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8") + b"\n"


def canonical_sha256(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def git(repo, *args, binary=False):
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()
        raise SourceTreeError(f"git {' '.join(args)} failed ({completed.returncode}): {detail}")
    return completed.stdout if binary else completed.stdout.decode("utf-8").rstrip("\n")


def repo_root(path):
    root = Path(git(path, "rev-parse", "--show-toplevel"))
    if root.resolve() != Path(path).resolve():
        raise SourceTreeError(f"{path} is not the top level of its Git work tree ({root})")
    return root


def status_entries(repo):
    """Parse porcelain v1 -z output into [{xy, path, original}] preserving order."""
    raw = git(repo, "status", "--porcelain=v1", "--untracked-files=all", "-z", binary=True)
    fields = raw.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()
    entries = []
    index = 0
    while index < len(fields):
        record = fields[index]
        index += 1
        if len(record) < 4 or record[2:3] != b" ":
            raise SourceTreeError(f"unparseable git status record: {record!r}")
        xy = record[:2].decode("ascii")
        path = record[3:].decode("utf-8", "surrogateescape")
        original = None
        if "R" in xy or "C" in xy:
            if index >= len(fields):
                raise SourceTreeError("rename record without original path")
            original = fields[index].decode("utf-8", "surrogateescape")
            index += 1
        entries.append({"xy": xy, "path": path, "original": original})
    return entries


def submodule_entries(repo):
    if not (Path(repo) / ".gitmodules").is_file():
        return []
    output = git(repo, "submodule", "status", "--recursive")
    entries = []
    for line in output.splitlines():
        if not line:
            continue
        prefix, rest = line[0], line[1:]
        parts = rest.split(" ", 2)
        if prefix not in " +-U" or len(parts) < 2:
            raise SourceTreeError(f"unparseable submodule status line: {line!r}")
        state = {" ": "clean", "+": "checkout_differs", "-": "not_initialized", "U": "merge_conflict"}[prefix]
        entries.append({"path": parts[1], "commit": parts[0], "state": state})
    return entries


def ls_files(repo):
    raw = git(repo, "ls-files", "-z", "-s", "--full-name", binary=True)
    entries = []
    for record in raw.split(b"\0"):
        if not record:
            continue
        meta, path = record.split(b"\t", 1)
        mode, _object, stage = meta.decode("ascii").split(" ")
        if stage != "0":
            raise SourceTreeError(f"unmerged index entry: {path!r}")
        entries.append((path, mode))
    entries.sort(key=lambda item: item[0])
    return entries


def hash_working_file(root, path, mode):
    target = Path(root) / os.fsdecode(path)
    if mode == "120000":
        return hashlib.sha256(os.readlink(target).encode("utf-8", "surrogateescape")).hexdigest()
    if mode == "160000":
        # A gitlink has no blob content in this tree; the submodule table carries its commit.
        return hashlib.sha256(b"").hexdigest()
    descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if info.st_size > MAX_TRACKED_FILE_BYTES:
            raise SourceTreeError(f"tracked file exceeds digest bound: {target}")
        hasher = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def tree_digest(repo):
    entries = []
    for path, mode in ls_files(repo):
        try:
            digest = hash_working_file(repo, path, mode)
        except FileNotFoundError:
            raise SourceTreeError(f"tracked file missing from the working tree: {path!r}") from None
        entries.append([path.decode("utf-8", "surrogateescape"), mode, digest])
    return canonical_sha256(entries), len(entries)


def lockfile_state(repo, status):
    tracked = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--error-unmatch", LOCKFILE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0
    touched = any(entry["path"] == LOCKFILE or entry["original"] == LOCKFILE for entry in status)
    diff = subprocess.run(
        ["git", "-C", str(repo), "diff", "--quiet", "HEAD", "--", LOCKFILE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode
    if diff not in (0, 1):
        raise SourceTreeError(f"git diff on {LOCKFILE} failed")
    return {"path": LOCKFILE, "tracked": tracked, "unchanged": tracked and diff == 0 and not touched}


def describe(repo, allow_unclean=False):
    root = repo_root(repo)
    status = status_entries(root)
    submodules = submodule_entries(root)
    lockfile = lockfile_state(root, status)
    if not lockfile["tracked"]:
        raise SourceTreeError(f"{LOCKFILE} must be tracked")
    if not lockfile["unchanged"]:
        raise SourceTreeError(f"{LOCKFILE} differs from HEAD; --locked builds require the committed lockfile")
    dirty_submodules = [entry for entry in submodules if entry["state"] != "clean"]
    tracked_changes = [entry for entry in status if entry["xy"] != "??"]
    untracked = [entry["path"] for entry in status if entry["xy"] == "??"]
    clean = not status and not dirty_submodules
    if not clean and not allow_unclean:
        problems = []
        if tracked_changes:
            problems.append(f"{len(tracked_changes)} tracked change(s): " + ", ".join(
                f"{entry['xy'].strip() or '??'} {entry['path']}" for entry in tracked_changes[:10]))
        if untracked:
            problems.append(f"{len(untracked)} untracked path(s): " + ", ".join(untracked[:10]))
        if dirty_submodules:
            problems.append("unclean submodule(s): " + ", ".join(entry["path"] for entry in dirty_submodules))
        raise SourceTreeError("fresh checkout required; " + "; ".join(problems))
    digest, count = tree_digest(root)
    return {
        "commit": git(root, "rev-parse", "HEAD"),
        "git_tree": git(root, "rev-parse", "HEAD^{tree}"),
        "tree_sha256": digest,
        "tree_digest_algorithm": "sha256(canonical_json([[path, gitmode, sha256(working_tree_content)]...]) + LF) over sorted git ls-files",
        "tracked_file_count": count,
        "clean": clean,
        "tracked_changes": [
            {"status": entry["xy"], "path": entry["path"], "original": entry["original"]} for entry in tracked_changes
        ],
        "untracked_paths": untracked,
        "submodules": submodules,
        "lockfile": lockfile,
    }


def _run_describe(args):
    report = describe(Path(args.repo), allow_unclean=args.dev_unclean_checkout)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.json_out:
        with open(args.json_out, "x", encoding="utf-8") as stream:
            stream.write(text)
    else:
        sys.stdout.write(text)
    if not report["clean"]:
        print("warning: DEVELOPMENT-ONLY unclean checkout admitted; manifest records source.clean=false", file=sys.stderr)


def _run_canonical_sha256(args):
    with open(args.input, "rb") if args.input != "-" else sys.stdin.buffer as stream:
        value = json.loads(stream.read().decode("utf-8"))
    sys.stdout.write(canonical_sha256(value) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command")
    describe_parser = subparsers.add_parser("describe", help="assert a fresh checkout and print the source description")
    describe_parser.add_argument("--repo", required=True, help="top level of the Git work tree")
    describe_parser.add_argument("--dev-unclean-checkout", action="store_true",
                                 help="DEVELOPMENT ONLY: admit tracked changes and untracked files, recording clean=false")
    describe_parser.add_argument("--json-out", help="write the description to this new file instead of stdout")
    describe_parser.set_defaults(run=_run_describe)
    digest_parser = subparsers.add_parser("canonical-sha256", help="print sha256(canonical_json(JSON) + LF)")
    digest_parser.add_argument("input", nargs="?", default="-", help="JSON file (default: stdin)")
    digest_parser.set_defaults(run=_run_canonical_sha256)
    args = parser.parse_args(argv)
    if not getattr(args, "run", None):
        parser.error("a subcommand is required")
    try:
        args.run(args)
    except SourceTreeError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
