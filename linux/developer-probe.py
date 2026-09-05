#!/usr/bin/env python3
"""Deterministic offline startup-probe rootfs from verified static BusyBox inputs.

No Docker, network, package manager or guest runtime is invoked. This artifact
supports a bounded startup usability probe; it is not the Docker release gate.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import struct
import tarfile
import tempfile


ARCHIVE = "developer-probe-rootfs.tar"
MARKER = b"vz-developer-probe-v1\n"
APPLETS = ("cat", "echo", "false", "printf", "sh", "sleep", "test", "true", "uname")
MAX_BUSYBOX_BYTES = 16 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_regular(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 < before.st_size <= limit,
                "bounded single-link regular input required")
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) and
                len(data) == before.st_size, "input changed while reading")
    return data


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical_sha(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "canonical SHA256 required")
    return value


def verify_static_arm64(binary):
    require(len(binary) >= 64 and binary[:7] == b"\x7fELF\x02\x01\x01", "Linux ELF64 little-endian binary required")
    require(struct.unpack_from("<HH", binary, 16) == (2, 183), "static ARM64 ET_EXEC required")
    offset = struct.unpack_from("<Q", binary, 32)[0]
    size, count = struct.unpack_from("<HH", binary, 54)
    require(size == 56 and 0 < count <= 128 and offset + size * count <= len(binary), "invalid ELF program headers")
    for index in range(count):
        kind = struct.unpack_from("<I", binary, offset + index * size)[0]
        require(kind not in (2, 3), "dynamic loader or dynamic program segment forbidden")


def rootfs_bytes(binary):
    verify_static_arm64(binary)
    entries = {"bin": (tarfile.DIRTYPE, 0o755, b"", ""),
               "bin/busybox": (tarfile.REGTYPE, 0o755, binary, ""),
               "etc": (tarfile.DIRTYPE, 0o755, b"", ""),
               "etc/vz-developer-probe": (tarfile.REGTYPE, 0o444, MARKER, ""),
               "tmp": (tarfile.DIRTYPE, 0o1777, b"", "")}
    entries.update({"bin/" + name: (tarfile.SYMTYPE, 0o777, b"", "busybox") for name in APPLETS})
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, (kind, mode, data, link) in sorted(entries.items()):
            member = tarfile.TarInfo(name)
            member.type, member.mode, member.size, member.linkname = kind, mode, len(data), link
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ""
            archive.addfile(member, io.BytesIO(data) if data else None)
    return output.getvalue()


def create(binary_path, provenance_path, expected_source_sha, busybox_version):
    binary = read_regular(binary_path, MAX_BUSYBOX_BYTES)
    provenance_bytes = read_regular(provenance_path, 4 * 1024 * 1024)
    provenance = json.loads(provenance_bytes)
    require(provenance["schema_version"] == 1 and provenance["build_parameters"]["kind"] == "busybox" and
            provenance["build_parameters"]["arch"] == "arm64", "wrong BusyBox build provenance")
    require(provenance["artifact_sha256"] == sha(binary), "BusyBox binary differs from build provenance")
    require(provenance["source"]["archive_sha256"] == canonical_sha(expected_source_sha), "unpinned BusyBox source archive")
    require(isinstance(busybox_version, str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", busybox_version), "BusyBox version required")
    require(provenance["source"]["archive_root"] == "busybox-" + busybox_version and
            provenance["source"]["case_sensitive_storage"] is True, "BusyBox source version/storage provenance differs")
    archive = rootfs_bytes(binary)
    metadata = {"schema_version": 1, "archive": ARCHIVE, "sha256": sha(archive), "busybox_sha256": sha(binary),
                "busybox_version": busybox_version, "source_archive_sha256": expected_source_sha,
                "source_inventory_sha256": canonical_sha(provenance["source"]["source_tree_sha256"]),
                "build_provenance_sha256": sha(provenance_bytes), "marker_sha256": sha(MARKER)}
    return archive, metadata


def publish(path, data):
    # Never follow an existing symlink/hardlink or recursively remove a source.
    if os.path.lexists(path):
        read_regular(path, 32 * 1024 * 1024)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".developer-probe-", delete=False) as stream:
        pending = Path(stream.name)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--busybox", required=True, type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--busybox-version", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()
    require(args.out_dir.is_dir() and not args.out_dir.is_symlink(), "existing non-symlink output directory required")
    archive, metadata = create(args.busybox, args.provenance, args.source_sha256, args.busybox_version)
    publish(args.out_dir / ARCHIVE, archive)
    publish(args.out_dir / "developer-probe.json", json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    print(json.dumps(metadata, sort_keys=True))


if __name__ == "__main__":
    main()
