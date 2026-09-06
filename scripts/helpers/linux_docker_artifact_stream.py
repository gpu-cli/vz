"""Read-only bounded artifact primitives, not OCI/layout/cache certification.

Accepted layer formats: uncompressed tar or one complete gzip member containing
tar; POSIX headers/PAX and GNU long name/link records. Nothing is extracted.
Host evidence links are forbidden; archive symlinks are inert reported metadata.
PAX support is limited to allowlisted standard text metadata keys; sparse/vendor
extensions (including xattrs) and concatenated gzip members are unsupported.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
import tarfile
import zlib


class ArtifactError(ValueError):
    """Static diagnostic codes deliberately exclude input paths and secret bytes."""


def _require(condition, code):
    if not condition:
        raise ArtifactError(code)


@dataclass(frozen=True)
class Limits:
    chunk_bytes: int = 65536
    file_bytes: int = 1024 * 1024 * 1024
    tree_bytes: int = 4 * 1024 * 1024 * 1024
    compressed_bytes: int = 1024 * 1024 * 1024
    uncompressed_bytes: int = 4 * 1024 * 1024 * 1024
    member_bytes: int = 1024 * 1024 * 1024
    metadata_bytes: int = 1024 * 1024
    entries: int = 20000
    depth: int = 64
    path_bytes: int = 4096

    def __post_init__(self):
        _require(all(type(value) is int and value > 0 for value in vars(self).values()), "invalid_limits")
        _require(self.chunk_bytes <= 1024 * 1024, "invalid_chunk_limit")


class CanaryScanner:
    """Scan complete streams, including canaries crossing chunk boundaries."""
    def __init__(self, canaries=()):
        selected, size = [], 0
        for canary in canaries:
            _require(len(selected) < 64 and type(canary) is bytes and 0 < len(canary) <= 65536,
                     "invalid_canaries")
            size += len(canary)
            _require(size <= 1024 * 1024, "canary_limit")
            selected.append(canary)
        self._canaries = tuple(selected)
        self._overlap = max((len(c) - 1 for c in self._canaries), default=0)
        self._tail = b""

    def feed(self, data):
        _require(type(data) is bytes, "invalid_scan_chunk")
        combined = self._tail + data
        _require(not any(c in combined for c in self._canaries), "secret_canary_detected")
        self._tail = combined[-self._overlap:] if self._overlap else b""


def _metadata(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_uid, info.st_gid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _identity(info):
    return info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)


@contextmanager
def _opened(path, directory=False):
    """Pin every ancestor, reject links, and recheck each name against its FD."""
    descriptors, edges = [], []
    try:
        raw = os.fspath(path)
        _require(isinstance(raw, str) and raw.startswith("/") and str(Path(raw)) == raw
                 and ".." not in Path(raw).parts, "noncanonical_path")
        components = Path(raw).parts[1:]
        _require(bool(components) and len(components) <= 128, "invalid_path_depth")
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
        current = os.open("/", flags | os.O_DIRECTORY)
        descriptors.append(current)
        for index, component in enumerate(components):
            is_dir = index < len(components) - 1 or directory
            selected = os.open(component, flags | (os.O_DIRECTORY if is_dir else 0), dir_fd=current)
            descriptors.append(selected)
            info = os.fstat(selected)
            _require(stat.S_ISDIR(info.st_mode) if is_dir else stat.S_ISREG(info.st_mode), "nonregular_source")
            if not is_dir:
                _require(info.st_nlink == 1, "hardlinked_source")
            edges.append((current, component, selected, _identity(info)))
            current = selected
        before = _metadata(os.fstat(current))
        yield current
        _require(_metadata(os.fstat(current)) == before, "source_changed")
        for parent, name, descriptor, identity in edges:
            _require(_identity(os.fstat(descriptor)) == identity and
                     _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) == identity,
                     "path_replaced")
    except (OSError, UnicodeError):
        raise ArtifactError("unsafe_or_unavailable_source") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _chunks(descriptor, maximum, limits, scanner, digest):
    size = os.fstat(descriptor).st_size
    _require(size <= maximum, "source_size_limit")
    count = 0
    while True:
        data = os.read(descriptor, limits.chunk_bytes)
        if not data:
            break
        count += len(data)
        _require(count <= maximum and count <= size, "source_size_changed")
        scanner.feed(data)
        digest.update(data)
        yield data
    _require(count == size, "source_size_changed")


def scan_file(path, *, canaries=(), limits=Limits()):
    """Hash and scan every byte of one stable, single-link regular file."""
    digest, scanner = hashlib.sha256(), CanaryScanner(canaries)
    with _opened(path) as descriptor:
        size = os.fstat(descriptor).st_size
        for _ in _chunks(descriptor, limits.file_bytes, limits, scanner, digest):
            pass
    return {"size": size, "sha256": digest.hexdigest()}


def inventory_tree(root, *, canaries=(), limits=Limits()):
    """Inventory a quiescent tree, checking signatures again after all hashing.

    This is change detection, not an atomic filesystem snapshot. The caller must
    retain producer quiescence while binding this inventory to later evidence.
    """
    canaries = CanaryScanner(canaries)._canaries
    files, directories, total, signatures = [], [], 0, {}

    def names_at(descriptor):
        names = []
        with os.scandir(descriptor) as entries:
            for entry in entries:
                _require(len(names) < limits.entries, "tree_entry_limit")
                names.append(entry.name)
        return sorted(names)

    def walk(descriptor, prefix, depth, verify=False):
        nonlocal total
        _require(depth <= limits.depth, "tree_depth_limit")
        before = _metadata(os.fstat(descriptor))
        if verify:
            _require(signatures.get(prefix) == before, "tree_directory_changed")
        else:
            signatures[prefix] = before
        names = names_at(descriptor)
        for name in names:
            if not verify:
                _require(len(files) + len(directories) < limits.entries, "tree_entry_limit")
            relative = prefix + name
            encoded = relative.encode("utf-8")
            _require(len(encoded) <= limits.path_bytes, "path_size_limit")
            CanaryScanner(canaries).feed(encoded)
            info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            directory = stat.S_ISDIR(info.st_mode)
            _require(directory or stat.S_ISREG(info.st_mode), "nonregular_tree_entry")
            flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
            child = os.open(name, flags | (os.O_DIRECTORY if directory else 0), dir_fd=descriptor)
            try:
                _require(_metadata(os.fstat(child)) == _metadata(info), "tree_entry_changed")
                key = relative + "/" if directory else relative
                if verify:
                    _require(signatures.get(key) == _metadata(info), "tree_entry_changed")
                else:
                    signatures[key] = _metadata(info)
                if directory:
                    if not verify:
                        directories.append({"path": relative, "mode": stat.S_IMODE(info.st_mode)})
                    walk(child, relative + "/", depth + 1, verify)
                else:
                    _require(info.st_nlink == 1, "hardlinked_source")
                    if not verify:
                        total += info.st_size
                        _require(total <= limits.tree_bytes, "tree_size_limit")
                        digest = hashlib.sha256()
                        for _ in _chunks(child, limits.file_bytes, limits, CanaryScanner(canaries), digest):
                            pass
                        files.append({"path": relative, "mode": stat.S_IMODE(info.st_mode),
                                      "size": info.st_size, "sha256": digest.hexdigest()})
                _require(_metadata(os.fstat(child)) == _metadata(info) and
                         _metadata(os.stat(name, dir_fd=descriptor, follow_symlinks=False)) == _metadata(info),
                         "tree_entry_changed")
            finally:
                os.close(child)
        _require(_metadata(os.fstat(descriptor)) == before and names_at(descriptor) == names,
                 "tree_directory_changed")
    with _opened(root, directory=True) as descriptor:
        walk(descriptor, "", 0)
        walk(descriptor, "", 0, verify=True)
    files.sort(key=lambda row: row["path"])
    directories.sort(key=lambda row: row["path"])
    result = {"files": files, "directories": directories, "total_bytes": total}
    result["inventory_sha256"] = hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":"),
                                                           ensure_ascii=False).encode()).hexdigest()
    return result


def _decoded(chunks, compression, limits):
    if compression == "uncompressed":
        yield from chunks
        return
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    for chunk in chunks:
        _require(not decoder.eof, "gzip_trailing_data")
        while chunk:
            data = decoder.decompress(chunk, limits.chunk_bytes)
            yield data
            _require(not decoder.unused_data, "gzip_trailing_data")
            chunk = decoder.unconsumed_tail
    _require(decoder.eof, "gzip_truncated")


class _Reader:
    def __init__(self, chunks, canaries, limits):
        self.chunks, self.scanner, self.limits = iter(chunks), CanaryScanner(canaries), limits
        self.buffer, self.size, self.digest = b"", 0, hashlib.sha256()

    def read(self, count):
        result = bytearray()
        while len(result) < count:
            if not self.buffer:
                try:
                    self.buffer = next(self.chunks)
                except StopIteration:
                    break
                self.size += len(self.buffer)
                _require(self.size <= self.limits.uncompressed_bytes, "uncompressed_size_limit")
                self.scanner.feed(self.buffer)
                self.digest.update(self.buffer)
                if not self.buffer:
                    continue
            take = min(count - len(result), len(self.buffer))
            result.extend(self.buffer[:take])
            self.buffer = self.buffer[take:]
        return bytes(result)

    def exact(self, count):
        data = self.read(count)
        _require(len(data) == count, "tar_truncated")
        return data

    def payload(self, count):
        digest = hashlib.sha256()
        while count:
            data = self.exact(min(count, self.limits.chunk_bytes))
            digest.update(data)
            count -= len(data)
        return digest.hexdigest()


def _path(value, limits, *, target=False):
    _require(isinstance(value, str) and "\x00" not in value and len(value.encode("utf-8")) <= limits.path_bytes,
             "invalid_archive_path")
    _require(not value.startswith("/") and ".." not in value.split("/"), "unsafe_archive_path")
    parts = [part for part in value.split("/") if part not in ("", ".")]
    _require(len(parts) <= limits.depth and (bool(parts) or not target), "invalid_archive_path")
    return "/".join(parts) or "."


def _pax(data):
    result = {}
    while data:
        length, separator, _ = data.partition(b" ")
        _require(separator and length.isdigit() and not length.startswith(b"0") and len(length) <= 10, "invalid_pax")
        count = int(length)
        _require(len(length) + 3 < count <= len(data), "invalid_pax")
        record, data = data[len(length) + 1:count], data[count:]
        key, equals, value = record[:-1].partition(b"=")
        _require(record.endswith(b"\n") and equals and key and b"\x00" not in key + value, "invalid_pax")
        key, value = key.decode("utf-8"), value.decode("utf-8")
        _require(key not in result and key in {"path", "linkpath", "size", "uid", "gid",
                 "uname", "gname", "mtime", "atime", "ctime", "comment"},
                 "unsupported_or_duplicate_pax")
        result[key] = value
    return result


def _tar(reader, limits, canaries):
    members, seen, overrides, global_pax = [], set(), {}, {}
    records = metadata_bytes = reported_bytes = 0
    while True:
        block = reader.exact(512)
        if block == b"\0" * 512:
            _require(reader.exact(512) == b"\0" * 512 and not overrides, "invalid_tar_end")
            while True:
                rest = reader.read(limits.chunk_bytes)
                if not rest:
                    break
                _require(not rest.strip(b"\0"), "tar_trailing_data")
            _require(reader.size % 512 == 0, "tar_truncated")
            # Symlink contents are inert, but no member or hardlink may traverse
            # a symlink in this archive. Hardlinks to prior-layer paths remain
            # metadata: resolving a complete overlay is the consumer's job.
            links = {row["path"] for row in members if row["type"] == "symlink"}
            for row in members:
                candidates = [row["path"]]
                if row["type"] == "hardlink":
                    candidates.append(row["link_target"])
                    _require(row["link_target"] not in links, "unsafe_archive_hardlink")
                for candidate in candidates:
                    parts = candidate.split("/")
                    _require(not any("/".join(parts[:i]) in links for i in range(1, len(parts))),
                             "archive_symlink_traversal")
            return members
        records += 1
        _require(records <= limits.entries, "tar_entry_limit")
        _require(block[257:265] in (tarfile.POSIX_MAGIC, tarfile.GNU_MAGIC), "unsupported_tar_format")
        header = tarfile.TarInfo.frombuf(block, "utf-8", "strict")
        _require(0 <= header.size <= limits.member_bytes, "member_size_limit")
        if header.type in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK):
            metadata_bytes += header.size
            _require(metadata_bytes <= limits.metadata_bytes, "metadata_size_limit")
            data = reader.exact(header.size)
            _require(not reader.exact((-header.size) % 512).strip(b"\0"), "invalid_tar_padding")
            if header.type in (tarfile.XHDTYPE, tarfile.XGLTYPE):
                updates = _pax(data)
                selected = global_pax if header.type == tarfile.XGLTYPE else overrides
                _require(not set(selected) & set(updates), "duplicate_pax_override")
                selected.update(updates)
            else:
                key = "path" if header.type == tarfile.GNUTYPE_LONGNAME else "linkpath"
                _require(key not in overrides and data.endswith(b"\0") and b"\0" not in data[:-1], "invalid_longname")
                overrides[key] = data[:-1].decode("utf-8")
            continue
        attrs = dict(global_pax, **overrides)
        overrides = {}
        name = _path(attrs.get("path", header.name), limits)
        _require(name not in seen, "duplicate_archive_path")
        seen.add(name)
        size = header.size
        if "size" in attrs:
            _require(attrs["size"].isascii() and attrs["size"].isdigit()
                     and len(attrs["size"]) <= 20, "invalid_pax_size")
            size = int(attrs["size"])
        _require(size <= limits.member_bytes, "member_size_limit")
        kinds = {tarfile.REGTYPE: "file", tarfile.AREGTYPE: "file", tarfile.DIRTYPE: "directory",
                 tarfile.SYMTYPE: "symlink", tarfile.LNKTYPE: "hardlink", tarfile.CHRTYPE: "character",
                 tarfile.BLKTYPE: "block", tarfile.FIFOTYPE: "fifo"}
        _require(header.type in kinds, "unsupported_tar_type")
        kind = kinds[header.type]
        _require(name != "." or kind == "directory", "invalid_archive_root")
        _require(kind == "file" or size == 0, "nonfile_payload")
        ownership = {}
        for key in ("uid", "gid"):
            value = attrs.get(key)
            if value is not None:
                _require(value.isascii() and value.isdigit() and len(value) <= 10,
                         "invalid_pax_ownership")
                value = int(value)
            else:
                value = getattr(header, key)
            _require(0 <= value <= 4294967295, "invalid_archive_ownership")
            ownership[key] = value
        _require(0 <= header.mode <= 0o7777, "invalid_archive_mode")
        row = {"path": name, "type": kind, "mode": header.mode,
               **ownership, "size": size, "pax": attrs}
        if kind in ("symlink", "hardlink"):
            link = attrs.get("linkpath", header.linkname)
            _require(link and "\x00" not in link and len(link.encode("utf-8")) <= limits.path_bytes, "invalid_link_target")
            row["link_target"] = _path(link, limits, target=True) if kind == "hardlink" else link
        if kind in ("character", "block"):
            _require(header.devmajor >= 0 and header.devminor >= 0, "invalid_archive_device")
            row.update(device_major=header.devmajor, device_minor=header.devminor)
        digest = reader.payload(size)
        if kind == "file":
            row["sha256"] = digest
        _require(not reader.exact((-size) % 512).strip(b"\0"), "invalid_tar_padding")
        for value in (name, row.get("link_target", ""), *attrs.keys(), *attrs.values()):
            CanaryScanner(canaries).feed(value.encode("utf-8"))
        reported_bytes += len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
        _require(reported_bytes <= limits.metadata_bytes, "reported_metadata_limit")
        members.append(row)


def scan_layer(path, *, compression, canaries=(), limits=Limits()):
    """Hash/scan complete compressed and uncompressed bytes plus inert tar metadata."""
    _require(compression in ("gzip", "uncompressed"), "unsupported_compression")
    canaries = CanaryScanner(canaries)._canaries
    compressed = hashlib.sha256()
    try:
        with _opened(path) as descriptor:
            size = os.fstat(descriptor).st_size
            chunks = _chunks(descriptor, limits.compressed_bytes, limits, CanaryScanner(canaries), compressed)
            reader = _Reader(_decoded(chunks, compression, limits), canaries, limits)
            members = _tar(reader, limits, canaries)
    except (tarfile.TarError, UnicodeError, zlib.error, OverflowError):
        raise ArtifactError("invalid_layer_encoding") from None
    return {"compressed_size": size, "compressed_sha256": compressed.hexdigest(),
            "uncompressed_size": reader.size, "diff_id": "sha256:" + reader.digest.hexdigest(), "members": members}
