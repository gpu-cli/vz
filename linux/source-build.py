#!/usr/bin/env python3
"""Pinned source extraction and build provenance; never repair an existing tree."""

import argparse
import collections
import contextlib
import copy
import email.utils
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile


@contextlib.contextmanager
def open_regular(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValueError(f"expected single-link regular file: {path}")
        yield stream


def read_regular(path):
    with open_regular(path) as stream:
        return stream.read()


def digest(path):
    with open_regular(path) as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".provenance-", delete=False) as stream:
        pending = Path(stream.name)
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())
    try:
        if os.path.lexists(path):
            with open_regular(path):
                pass
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)


def require_case_sensitive(parent):
    """Probe an owned fresh directory, without touching preexisting sources."""
    parent = Path(parent)
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".case-probe-", dir=parent) as temporary:
        first, second = Path(temporary) / "Case", Path(temporary) / "case"
        first.write_bytes(b"upper")
        try:
            with second.open("xb") as stream:
                stream.write(b"lower")
        except FileExistsError as error:
            raise ValueError(
                f"case-sensitive source/build storage required: {parent}; use docker-build"
            ) from error
        if first.stat().st_ino == second.stat().st_ino or first.read_bytes() != b"upper":
            raise ValueError(f"case-distinct files are not preserved: {parent}")


def archive_inventory(archive, expected_sha, root_name):
    archive = Path(archive)
    if archive.is_symlink() or not archive.is_file():
        raise ValueError("source archive must be a regular non-symlink file")
    if not re.fullmatch(r"[a-f0-9]{64}", expected_sha) or digest(archive) != expected_sha:
        raise ValueError(f"source archive checksum mismatch: {archive}")
    entries = {}
    with tarfile.open(archive, "r|*") as source:
        for member in source:
            name = PurePosixPath(member.name)
            if name.is_absolute() or ".." in name.parts or not name.parts or name.parts[0] != root_name:
                raise ValueError(f"unsafe source archive path: {member.name}")
            relative = str(PurePosixPath(*name.parts[1:]))
            if relative in entries:
                raise ValueError(f"duplicate source archive path: {member.name}")
            entry = {"path": relative, "mode": member.mode & 0o777, "mtime": int(member.mtime)}
            if member.isdir():
                entry["type"] = "directory"
            elif member.isfile():
                entry.update(type="file", size=member.size)
                with source.extractfile(member) as stream:
                    entry["sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
            elif member.issym():
                target = PurePosixPath(member.linkname)
                resolved = os.path.normpath(str(name.parent / target))
                if target.is_absolute() or resolved != root_name and not resolved.startswith(root_name + "/"):
                    raise ValueError(f"unsafe source symlink: {member.name}")
                entry.update(type="symlink", target=member.linkname)
            else:
                raise ValueError(f"unsupported source archive entry: {member.name}")
            entries[relative] = entry
    if "." not in entries or entries["."]["type"] != "directory":
        raise ValueError("source archive is missing its root directory")
    for entry in entries.values():
        for ancestor in PurePosixPath(entry["path"]).parents:
            if str(ancestor) in entries and entries[str(ancestor)]["type"] != "directory":
                raise ValueError(f"source member descends through non-directory: {entry['path']}")
    return sorted(entries.values(), key=lambda entry: entry["path"])


def verify_tree(root, entries):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"source tree must be a real directory: {root}")
    actual_names = {"."}
    for directory, directories, files in os.walk(root, followlinks=False):
        actual_names.update(str((Path(directory) / name).relative_to(root)) for name in directories + files)
    if actual_names != {entry["path"] for entry in entries}:
        raise ValueError("source tree inventory differs from pinned archive")
    case_groups = collections.defaultdict(list)
    for entry in entries:
        path = root / entry["path"]
        metadata = path.lstat()
        kind = entry["type"]
        if kind == "file":
            valid = stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1
            valid = valid and metadata.st_size == entry["size"] and digest(path) == entry["sha256"]
        elif kind == "directory":
            valid = stat.S_ISDIR(metadata.st_mode)
        else:
            valid = stat.S_ISLNK(metadata.st_mode) and os.readlink(path) == entry["target"]
        if not valid or kind != "symlink" and stat.S_IMODE(metadata.st_mode) != entry["mode"]:
            raise ValueError(f"source content/type/mode mismatch: {entry['path']}")
        case_groups[entry["path"].casefold()].append((entry, metadata.st_dev, metadata.st_ino))
    collisions = []
    for group in case_groups.values():
        if len(group) > 1:
            if len({(item[1], item[2]) for item in group}) != len(group):
                raise ValueError("case-distinct source paths share a filesystem inode")
            collisions.append([item[0] for item in group])
    return sorted(collisions, key=lambda group: group[0]["path"])


def prepare_source(archive, expected_sha, root_name, destination, build_dir):
    destination, build_dir = Path(destination), Path(build_dir)
    if not destination.is_absolute() or not build_dir.is_absolute():
        raise ValueError("source and build directories must be absolute")
    if destination == Path(destination.anchor) or build_dir == Path(build_dir.anchor):
        raise ValueError("source and build require dedicated directories")
    if destination == build_dir or destination in build_dir.parents or build_dir in destination.parents:
        raise ValueError("source and build directories must be disjoint")
    require_case_sensitive(destination.parent)
    require_case_sensitive(build_dir)
    entries = archive_inventory(archive, expected_sha, root_name)
    lock_path = destination.parent / ".source-build.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not destination.exists() and not destination.is_symlink():
            with tempfile.TemporaryDirectory(prefix=".source-extract-", dir=destination.parent) as pending:
                subprocess.run(["tar", "--no-same-owner", "-xpf", str(Path(archive).resolve()), "-C", pending], check=True)
                verify_tree(Path(pending) / root_name, entries)
                os.rename(Path(pending) / root_name, destination)
        collisions = verify_tree(destination, entries)
        manifest = {
            "schema_version": 1,
            "archive_sha256": expected_sha,
            "archive_root": root_name,
            "source_tree_sha256": hashlib.sha256(encoded(entries)).hexdigest(),
            "entry_count": len(entries),
            "source_date_epoch": max(entry["mtime"] for entry in entries),
            "case_sensitive_storage": True,
            "case_distinct_groups": collisions,
        }
        manifest_path = Path(str(destination) + ".source.json")
        if manifest_path.exists():
            if manifest_path.is_symlink() or read_regular(manifest_path) != encoded(manifest):
                raise ValueError("existing source provenance differs; no source or evidence was replaced")
        else:
            write_json(manifest_path, manifest)
        return manifest


def recipe_identity(paths):
    result = {Path(path).name: digest(path) for path in paths}
    if len(result) != len(paths):
        raise ValueError("recipe input basenames must be unique")
    return result


def build_parameters(args):
    return {
        "kind": args.kind,
        "arch": args.arch,
        "cross_compile": args.cross_compile,
        "base_config": args.base_config if args.kind == "kernel" else "defconfig",
        "build_user": "vz",
        "build_host": "vz-linux-builder",
        "build_version": "1",
    }


def compiler_identity(compiler):
    return subprocess.check_output([compiler, "--version"], text=True, env=build_environment(0)).strip()


def build_environment(source_date_epoch):
    # Do not inherit make/compiler/linker overrides. In particular, MAKEFLAGS
    # and MAKEOVERRIDES can otherwise reintroduce arbitrary parent make inputs
    # even after CC/KCFLAGS are removed. Keep only ordinary process locations.
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR", "TMP", "TEMP") if name in os.environ}
    environment.update(
        LC_ALL="C", LANG="C", TZ="UTC",
        SOURCE_DATE_EPOCH=str(source_date_epoch),
        KBUILD_BUILD_TIMESTAMP=email.utils.formatdate(source_date_epoch, usegmt=True),
        KBUILD_BUILD_USER="vz", KBUILD_BUILD_HOST="vz-linux-builder", KBUILD_BUILD_VERSION="1",
    )
    return environment


def record_artifact(args):
    artifact = Path(args.artifact)
    if artifact.is_symlink() or not artifact.is_file() or artifact.stat().st_size == 0:
        raise ValueError("build artifact must be a nonempty regular file")
    source = json.loads(read_regular(args.source_manifest))
    if source.get("case_sensitive_storage") is not True or source.get("schema_version") != 1:
        raise ValueError("missing verified source provenance")
    result = {
        "schema_version": 1,
        "profile": args.profile,
        "artifact_sha256": digest(artifact),
        "artifact_size": artifact.stat().st_size,
        "artifact_mode": stat.S_IMODE(artifact.stat().st_mode),
        "source": source,
        "config_fragment_sha256": digest(args.config) if args.config else None,
        "effective_config_sha256": digest(args.effective_config),
        "effective_config": read_regular(args.effective_config).decode("utf-8"),
        "recipe_inputs": recipe_identity(args.recipe),
        "build_parameters": build_parameters(args),
        "builder_id": os.environ.get("VZ_LINUX_BUILDER_ID") or f"native:{platform.system()}:{platform.machine()}",
        "compiler": compiler_identity(args.compiler),
    }
    write_json(str(artifact) + ".build.json", result)


def verify_artifact(args):
    artifact = Path(args.artifact)
    evidence = Path(str(artifact) + ".build.json")
    if artifact.is_symlink() or evidence.is_symlink():
        raise ValueError("cached artifacts/provenance must not be symlinks")
    result = json.loads(read_regular(evidence))
    expected_config = digest(args.config) if args.config else None
    if (
        result.get("schema_version") != 1
        or result.get("profile") != args.profile
        or result.get("artifact_sha256") != digest(artifact)
        or result.get("artifact_size") != artifact.stat().st_size
        or result.get("artifact_mode") != stat.S_IMODE(artifact.stat().st_mode)
        or result.get("artifact_mode") != (0o755 if args.kind == "busybox" else 0o644)
        or result.get("config_fragment_sha256") != expected_config
        or result.get("recipe_inputs") != recipe_identity(args.recipe)
        or result.get("build_parameters") != build_parameters(args)
        or result.get("source", {}).get("archive_sha256") != args.sha256
        or result.get("source", {}).get("case_sensitive_storage") is not True
        or not result.get("source", {}).get("source_tree_sha256")
        or not isinstance(result.get("source", {}).get("source_date_epoch"), int)
        or not result.get("effective_config_sha256")
        or not isinstance(result.get("effective_config"), str)
        or hashlib.sha256(result["effective_config"].encode()).hexdigest() != result["effective_config_sha256"]
        or not result.get("builder_id")
        or os.environ.get("VZ_LINUX_BUILDER_ID") and result.get("builder_id") != os.environ["VZ_LINUX_BUILDER_ID"]
        or result.get("compiler") != compiler_identity(args.cross_compile + "gcc")
    ):
        raise ValueError("cached build provenance does not match current pinned inputs and artifact")


def build_artifact(args):
    source, build = Path(args.source), Path(args.build_dir)
    require_case_sensitive(build)
    descriptor = os.open(build / ".build.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if Path(args.artifact).exists():
            try:
                verify_artifact(args)
                print(f"Verified cached {args.kind} artifact and source provenance: {args.artifact}")
                return
            except (OSError, ValueError) as error:
                print(f"Rebuilding {args.kind}; cached provenance is not current: {error}", flush=True)
        make = ["make", "-C", str(source), "O=" + str(build), "ARCH=" + args.arch,
                "CROSS_COMPILE=" + args.cross_compile]
        manifest = json.loads(read_regular(str(source) + ".source.json"))
        environment = build_environment(manifest["source_date_epoch"])
        subprocess.run(make + [args.base_config if args.kind == "kernel" else "defconfig"], env=environment, check=True)
        effective = build / ".config"
        if args.kind == "kernel":
            subprocess.run([str(source / "scripts/kconfig/merge_config.sh"), "-m", "-O", str(build),
                            str(effective), args.config], cwd=build, env=environment, check=True)
            subprocess.run(make + ["olddefconfig"], env=environment, check=True)
            target, binary = "Image", build / "arch/arm64/boot/Image"
        else:
            config = effective.read_text().replace("# CONFIG_STATIC is not set", "CONFIG_STATIC=y")
            for feature in ("SHA1_HWACCEL", "SHA256_HWACCEL"):
                config = config.replace(f"CONFIG_{feature}=y", f"# CONFIG_{feature} is not set")
            effective.write_text(config)
            subprocess.run(make + ["oldconfig"], input="\n" * 20000, text=True, env=environment, check=True)
            target, binary = "busybox", build / "busybox"
        subprocess.run(make + ["-j" + str(args.jobs), target], env=environment, check=True)
        # Out-of-tree builds must not silently modify pinned source bytes.
        entries = archive_inventory(args.archive, args.sha256, manifest["archive_root"])
        verify_tree(source, entries)
        output = Path(args.artifact)
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=output.parent, prefix=".source-built-") as staging:
            pending = Path(staging) / output.name
            shutil.copyfile(binary, pending)
            pending.chmod(0o755 if args.kind == "busybox" else 0o644)
            candidate = copy.copy(args)
            candidate.artifact = str(pending)
            candidate.source_manifest = str(source) + ".source.json"
            candidate.effective_config = str(effective)
            candidate.compiler = args.cross_compile + "gcc"
            # Finish every provenance read, write, and validation before replacing
            # an existing artifact. A compiler/config/evidence failure is no-op
            # for the published output, rather than losing the previous build.
            record_artifact(candidate)
            verify_artifact(candidate)
            publish_artifact(pending, output)


def publish_artifact(pending, output):
    pending, output = Path(pending), Path(output)
    evidence = Path(str(output) + ".build.json")
    descriptor = os.open(output.parent / ".source-build-output.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        previous = [path for path in (output, evidence) if os.path.lexists(path)]
        for path in previous:
            with open_regular(path):
                pass
        if previous:
            # Two path replacements cannot form one filesystem transaction. Keep
            # an owned recovery copy of the old pair before publishing either;
            # a later I/O failure must not make the previous bytes unrecoverable.
            retained = Path(tempfile.mkdtemp(prefix=f".previous-{output.name}-", dir=output.parent))
            for path in previous:
                shutil.copyfile(path, retained / path.name)
                (retained / path.name).chmod(stat.S_IMODE(path.stat().st_mode))
                with open_regular(retained / path.name) as stream:
                    os.fsync(stream.fileno())
            sync_directory(retained)
            sync_directory(output.parent)
            print(f"Retained previous artifact/provenance for recovery: {retained}", flush=True)
        for path in (pending, Path(str(pending) + ".build.json")):
            with open_regular(path) as stream:
                os.fsync(stream.fileno())
        os.replace(pending, output)
        os.replace(str(pending) + ".build.json", evidence)
        sync_directory(output.parent)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    for name in ("archive", "sha256", "root-name", "destination", "build-dir"):
        prepare.add_argument("--" + name, required=True)
    probe = commands.add_parser("probe")
    probe.add_argument("directory")
    for command in ("record", "verify", "build"):
        item = commands.add_parser(command)
        item.add_argument("--artifact", required=True)
        item.add_argument("--profile", choices=("developer", "container"), required=True)
        item.add_argument("--config")
        item.add_argument("--recipe", action="append", required=True)
        item.add_argument("--kind", choices=("kernel", "busybox"), required=True)
        item.add_argument("--arch", default="arm64")
        item.add_argument("--cross-compile", default="")
        item.add_argument("--base-config", default="defconfig")
        if command == "build":
            for name in ("source", "build-dir", "archive", "sha256"):
                item.add_argument("--" + name, required=True)
            item.add_argument("--jobs", type=int, required=True)
        elif command == "record":
            item.add_argument("--source-manifest", required=True)
            item.add_argument("--effective-config", required=True)
            item.add_argument("--compiler", required=True)
        else:
            item.add_argument("--sha256", required=True)
    args = parser.parse_args()
    if args.command in ("record", "verify", "build"):
        if args.arch != "arm64" or (args.kind == "kernel") != bool(args.config):
            raise ValueError("arm64 builds require a config fragment for kernel only")
    if args.command == "prepare":
        result = prepare_source(args.archive, args.sha256, args.root_name, args.destination, args.build_dir)
        print(json.dumps({
            "archive_sha256": result["archive_sha256"],
            "source_tree_sha256": result["source_tree_sha256"],
            "entry_count": result["entry_count"],
            "case_distinct_group_count": len(result["case_distinct_groups"]),
            "manifest": args.destination + ".source.json",
        }, sort_keys=True))
    elif args.command == "probe":
        require_case_sensitive(args.directory)
    elif args.command == "record":
        record_artifact(args)
    elif args.command == "build":
        if args.jobs < 1:
            raise ValueError("positive build parallelism required")
        build_artifact(args)
    else:
        verify_artifact(args)


if __name__ == "__main__":
    main()
