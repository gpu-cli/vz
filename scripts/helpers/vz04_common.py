"""Shared primitives for the vz 0.4 aggregate release gate.

Digests, strict JSON, fsync'd exclusive writes, bounded tree digests and git
queries. Nothing here provisions, signs or certifies anything. Patterns are
taken from `installed_developer_startup.py` (bounded single-link SHA-256,
`write`/`document`), `docker_compatibility_contract.py` (strict JSON, no-follow
reads) and `docker_host_driver.py` (tree digest, checked text).
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import time
from datetime import datetime, timezone

REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT_DEFAULT = ".artifacts/vz-0.4-e2e"
RUN_ID_PATTERN = r"[a-z0-9][a-z0-9-]{7,63}"
DIGEST_PATTERN = r"[0-9a-f]{64}"
CANARY_PREFIX = "vz04-canary-"
MAX_JSON = 4 * 1024 * 1024
MAX_FILE = 2 * 1024 ** 3
MAX_TREE_ENTRIES = 20000
MAX_TREE_BYTES = 4 * 1024 ** 3
EXCLUDED_TREE_DIRS = frozenset(("__pycache__",))

CONFIG_FILES = {
    "e2e_contract": "config/vz-0.4-e2e-contract.json",
    "docker_contract": "config/docker-compatibility-v0.4.json",
    "migration_barriers": "config/vz-0.4-migration-barriers.json",
    "decisions": "config/vz-0.4-decisions.json",
    "decision_authorities": "config/vz-0.4-decision-authorities.json",
    "host_target_capabilities": "config/host-target-capabilities-v0.4.json",
}
DECISION_SIGNATURES_DIR = "config/vz-0.4-decision-signatures"
SCHEMAS_DIR = "schemas"
PHASES = ("clean-provision", "persisted-recovery", "final-cleanup")
LANE_PHASES = ("clean-provision", "persisted-recovery/pre-sleep",
               "persisted-recovery/post-wake", "final-cleanup")


class GateError(Exception):
    """Input admission or integrity failure. Never a workload result."""


def require(condition, message):
    if not condition:
        raise GateError(message)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(value) -> str:
    """Deterministic JSON: sorted keys, no whitespace, UTF-8 preserved."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def canonical_digest(value) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def now_ns() -> int:
    return time.time_ns()


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def checked_text(value, pattern: str, name: str) -> str:
    require(isinstance(value, str) and re.fullmatch(pattern, value) is not None, f"invalid {name}: {value!r}")
    return value


def _read_descriptor(descriptor, limit, what):
    before = os.fstat(descriptor)
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, f"{what}: not a single-link regular file")
    require(before.st_size <= limit, f"{what}: exceeds byte bound {limit}")
    hasher = hashlib.sha256()
    chunks = []
    observed = 0
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        observed += len(chunk)
        require(observed <= before.st_size, f"{what}: grew during read")
        hasher.update(chunk)
        chunks.append(chunk)
    after = os.fstat(descriptor)
    require(observed == before.st_size and
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
            f"{what}: changed during read")
    return b"".join(chunks), hasher.hexdigest(), stat.S_IMODE(before.st_mode)


def read_regular(path: Path, limit: int = MAX_FILE) -> bytes:
    """Bounded no-follow read of a single-link regular file."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise GateError(f"cannot open regular file {path}: {error.strerror}") from error
    try:
        return _read_descriptor(descriptor, limit, str(path))[0]
    finally:
        os.close(descriptor)


def digest_file(path: Path, limit: int = MAX_FILE) -> str:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise GateError(f"cannot open regular file {path}: {error.strerror}") from error
    try:
        return _read_descriptor(descriptor, limit, str(path))[1]
    finally:
        os.close(descriptor)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON object member: {key}")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise GateError("non-finite JSON number")


def parse_strict_json(data: bytes, what: str = "input"):
    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GateError(f"{what} is not strict UTF-8 JSON: {error}") from error


def load_json(path: Path, limit: int = MAX_JSON):
    return parse_strict_json(read_regular(path, limit), str(path))


def write_exclusive(path: Path, data: bytes) -> None:
    """Exclusive create, fsync file and directory. Refuses to overwrite."""
    with open(path, "xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def write_replace(path: Path, data: bytes) -> None:
    """Atomic replace via temp file + rename, fsync'd. Used only for the two
    documents the gate is allowed to rewrite (manifest verdict, run index)."""
    temporary = path.with_name(path.name + ".tmp")
    if temporary.exists():
        temporary.unlink()
    write_exclusive(temporary, data)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def document(path: Path, value, replace: bool = False) -> None:
    data = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    (write_replace if replace else write_exclusive)(path, data)


def relative_components(relative: str) -> tuple:
    require(isinstance(relative, str), "path must be text")
    path = PurePosixPath(relative)
    require(not path.is_absolute() and str(path) == relative and path.parts and
            all(part not in (".", "..") for part in path.parts), f"path is not canonical and repository-relative: {relative}")
    return path.parts


def canonical_path(value, must_exist: bool = True) -> Path:
    path = Path(value)
    require(path.is_absolute() and not any(c in str(path) for c in "\r\n\x00"), f"absolute clean path required: {value}")
    if not must_exist:
        return path
    resolved = path.resolve(strict=True)
    require(resolved == path, f"canonical path required (symlink or relative component): {path}")
    return resolved


def tree_entries(root: Path, excluded_dirs=EXCLUDED_TREE_DIRS) -> list:
    """Sorted [relative-path, mode, size, sha256] rows over a real directory.

    Symlinks, special files and hardlinks are rejected. Directories named in
    `excluded_dirs` (build products such as __pycache__) are not inventoried.
    """
    require(root.is_dir() and not root.is_symlink(), f"tree root is not a real directory: {root}")
    rows = []
    total = 0

    def walk(directory: Path, prefix: tuple, depth: int):
        nonlocal total
        require(depth <= 32, "tree depth exceeds bound")
        with os.scandir(directory) as scan:
            names = sorted(entry.name for entry in scan)
        for name in names:
            child = directory / name
            metadata = os.lstat(child)
            if stat.S_ISDIR(metadata.st_mode):
                if name in excluded_dirs:
                    continue
                walk(child, (*prefix, name), depth + 1)
                continue
            require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1,
                    f"tree contains a symlink, special file or hardlink: {child}")
            descriptor = os.open(child, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                _data, digest, mode = _read_descriptor(descriptor, MAX_FILE, str(child))
            finally:
                os.close(descriptor)
            total += metadata.st_size
            require(total <= MAX_TREE_BYTES, "tree bytes exceed bound")
            rows.append(["/".join((*prefix, name)), mode, metadata.st_size, digest])
            require(len(rows) <= MAX_TREE_ENTRIES, "tree entry count exceeds bound")

    walk(root, (), 0)
    return sorted(rows)


def tree_digest(root: Path, allow_empty: bool = False) -> str:
    rows = tree_entries(root)
    require(allow_empty or rows, f"empty tree: {root}")
    return canonical_digest(rows)


def files_digest(repo_root: Path, relative_paths) -> dict:
    """Digest of an explicit file list: {path: sha256} plus the aggregate."""
    per_file = {}
    for relative in relative_paths:
        relative_components(relative)
        per_file[relative] = digest_file(repo_root / relative)
    return {"files": per_file, "sha256": canonical_digest(sorted(per_file.items()))}


def write_checksums(root: Path, name: str = "checksums.sha256") -> Path:
    rows = []
    for relative, _mode, _size, digest in tree_entries(root, excluded_dirs=frozenset()):
        if relative != name:
            rows.append(f"{digest}  {relative}\n")
    target = root / name
    write_exclusive(target, "".join(rows).encode("utf-8"))
    return target


def verify_checksums(root: Path, name: str = "checksums.sha256") -> list:
    """Return findings; empty when every file is covered and matches."""
    findings = []
    target = root / name
    if not target.is_file() or target.is_symlink():
        return [f"{name} missing"]
    declared = {}
    for line in read_regular(target, MAX_JSON).decode("utf-8").splitlines():
        try:
            digest, relative = line.split("  ", 1)
            checked_text(digest, DIGEST_PATTERN, "checksum digest")
        except (ValueError, GateError):
            findings.append(f"{name}: malformed row {line!r}")
            continue
        if relative in declared:
            findings.append(f"{name}: duplicate row {relative}")
        declared[relative] = digest
    actual = {relative: digest for relative, _m, _s, digest in tree_entries(root, excluded_dirs=frozenset())
              if relative != name}
    for relative in sorted(set(declared) - set(actual)):
        findings.append(f"{name}: declared file absent {relative}")
    for relative in sorted(set(actual) - set(declared)):
        findings.append(f"{name}: file not covered {relative}")
    for relative in sorted(set(actual) & set(declared)):
        if actual[relative] != declared[relative]:
            findings.append(f"{name}: digest mismatch {relative}")
    return findings


def git(repo_root: Path, *args, check: bool = True) -> str:
    completed = subprocess.run(["git", "-C", str(repo_root), *args], stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, check=False)
    if check and completed.returncode != 0:
        raise GateError(f"git {' '.join(args)} failed: {completed.stderr.decode('utf-8', 'replace').strip()}")
    return completed.stdout.decode("utf-8", "replace").strip()


def git_head(repo_root: Path) -> str:
    return checked_text(git(repo_root, "rev-parse", "HEAD"), r"[0-9a-f]{40}", "commit")


def git_dirty(repo_root: Path) -> bool:
    return bool(git(repo_root, "status", "--porcelain=v1", "--untracked-files=all"))


def git_is_strict_ancestor(repo_root: Path, ancestor: str, descendant: str) -> bool:
    if ancestor == descendant:
        return False
    completed = subprocess.run(["git", "-C", str(repo_root), "merge-base", "--is-ancestor", ancestor, descendant],
                               stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    return completed.returncode == 0


def which(name: str):
    for directory in ("/usr/bin", "/bin", "/usr/sbin", "/sbin", "/opt/homebrew/bin", "/usr/local/bin"):
        candidate = Path(directory) / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None
