"""Inert, bounded scan of a stopped worker's Docker-cp TAR.

No extraction, subprocess, path following inside archives, or private canary
serialization. This proves absence of supplied literal canaries in raw bytes
and the explicitly supported recursively decoded formats, not arbitrary
encryption/encoding, deleted disk sectors, or a live filesystem snapshot.
The caller must separately prove exact owner, quiescence, and capture closure.
"""
import bz2
from dataclasses import dataclass
import hashlib
import io
import json
import lzma
import os
import stat
import struct
import tarfile
import zipfile
import zlib

import linux_docker_artifact_stream as stream


class CacheError(ValueError):
    """Fixed diagnostics contain neither member names nor private bytes."""


def require(ok, code):
    if not ok:
        raise CacheError(code)


@dataclass(frozen=True)
class Limits:
    archive_bytes: int = 4 * 1024**3
    decoded_bytes: int = 8 * 1024**3
    member_bytes: int = 1024**3
    buffered_bytes: int = 128 * 1024**2
    metadata_bytes: int = 8 * 1024**2
    entries: int = 100000
    depth: int = 12
    chunk_bytes: int = 65536

    def __post_init__(self):
        require(all(type(v) is int and v > 0 for v in vars(self).values()), "invalid_limits")
        require(self.chunk_bytes <= 1024**2 and self.depth <= 64, "invalid_limits")


class _Input:
    def __init__(self, chunks, state):
        self.chunks = iter(chunks)
        self.state = state
        self.tail = b""
        self.size = 0
        self.digest = hashlib.sha256()
        self.scanner = stream.CanaryScanner(state.canaries)

    def read(self, count):
        out = bytearray()
        while len(out) < count:
            if not self.tail:
                try:
                    self.tail = next(self.chunks)
                except StopIteration:
                    break
                require(type(self.tail) is bytes, "invalid_chunk")
                self.scanner.feed(self.tail)
                self.digest.update(self.tail)
                self.size += len(self.tail)
                self.state.decoded += len(self.tail)
                require(self.state.decoded <= self.state.limits.decoded_bytes, "aggregate_decoded_limit")
                if not self.tail:
                    continue
            take = min(count - len(out), len(self.tail))
            out.extend(self.tail[:take])
            self.tail = self.tail[take:]
        return bytes(out)

    def exact(self, count):
        out = self.read(count)
        require(len(out) == count, "truncated_member")
        return out

    def remaining(self):
        while True:
            out = self.read(self.state.limits.chunk_bytes)
            if not out:
                return
            yield out


def _bounded(reader, size):
    while size:
        data = reader.exact(min(size, reader.state.limits.chunk_bytes))
        size -= len(data)
        yield data


def _join(prefix, rest):
    yield prefix
    yield from rest


def _decode(chunks, kind, limits):
    if kind == "gzip":
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    elif kind == "xz":
        decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=limits.buffered_bytes)
    else:
        decoder = bz2.BZ2Decompressor()
    for chunk in chunks:
        require(not decoder.eof, "compressed_trailing_data")
        if kind == "gzip":
            while chunk:
                data = decoder.decompress(chunk, limits.chunk_bytes)
                require(not decoder.unused_data, "compressed_trailing_data")
                yield data
                chunk = decoder.unconsumed_tail
        else:
            data = decoder.decompress(chunk, max_length=limits.chunk_bytes)
            yield data
            while not decoder.eof and not decoder.needs_input:
                yield decoder.decompress(b"", max_length=limits.chunk_bytes)
            require(not decoder.unused_data, "compressed_trailing_data")
    require(decoder.eof, "compressed_truncated")


def _kind(prefix):
    if prefix.startswith(b"\x1f\x8b"):
        return "gzip"
    if prefix.startswith(b"\xfd7zXZ\x00"):
        return "xz"
    if prefix.startswith(b"BZh"):
        return "bzip2"
    if prefix.startswith(b"!<arch>\n"):
        return "ar"
    if prefix.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return "zip"
    require(b"PK\x03\x04" not in prefix, "unsupported_prefixed_zip")
    if len(prefix) >= 265 and prefix[257:265] in (tarfile.POSIX_MAGIC, tarfile.GNU_MAGIC):
        return "tar"
    # Never certify common unsupported compression/archive signatures as raw.
    magic = int.from_bytes(prefix[:4], "little") if len(prefix) >= 4 else 0
    require(not (magic == 0xfd2fb528 or magic & 0xfffffff0 == 0x184d2a50), "unsupported_zstd")
    require(not prefix.startswith((b"\x04\x22\x4d\x18", b"LZIP", b"7z\xbc\xaf\x27\x1c",
                                  b"Rar!", b"\x1f\x9d", b"\x1f\xa0")), "unsupported_encoding")
    return "raw"


class _State:
    def __init__(self, canaries, limits):
        self.canaries = stream.CanaryScanner(canaries)._canaries
        require(bool(self.canaries), "private_canaries_required")
        self.limits = limits
        self.decoded = self.entries = self.metadata = 0
        self.encodings = set()

    def note(self, item):
        self.entries += 1
        self.metadata += len(json.dumps(item, sort_keys=True).encode())
        require(self.entries <= self.limits.entries, "aggregate_entry_limit")
        require(self.metadata <= self.limits.metadata_bytes, "aggregate_metadata_limit")

    def scan(self, chunks, depth=0, *, outer=False):
        require(depth <= self.limits.depth, "encoding_depth_limit")
        source = _Input(chunks, self)
        prefix = source.read(512)
        kind = _kind(prefix)
        require(not outer or kind == "tar", "outer_tar_required")
        self.encodings.add(kind)
        data = _join(prefix, source.remaining())
        proof = {"encoding": kind}
        if kind in ("gzip", "xz", "bzip2"):
            proof["decoded"] = self.scan(_decode(data, kind, self.limits), depth + 1)
        elif kind == "tar":
            proof["members"] = self.tar(data, depth)
        elif kind == "ar":
            proof["members"] = self.ar(data, depth)
        elif kind == "zip":
            proof["members"] = self.zip(data, depth)
        else:
            for _ in data:
                pass
        require(not source.read(1), "member_not_fully_consumed")
        proof.update(size=source.size, sha256=source.digest.hexdigest())
        return proof

    def tar(self, chunks, depth):
        state = self
        policy = stream.Limits(chunk_bytes=self.limits.chunk_bytes,
            member_bytes=self.limits.member_bytes, uncompressed_bytes=self.limits.archive_bytes,
            entries=self.limits.entries, metadata_bytes=self.limits.metadata_bytes)
        class Reader(stream._Reader):
            def payload(self, count):
                def pieces():
                    remaining = count
                    while remaining:
                        block = self.exact(min(remaining, policy.chunk_bytes))
                        remaining -= len(block)
                        yield block
                proof = state.scan(pieces(), depth + 1)
                self.payloads.append(proof)
                return proof["sha256"]
        reader = Reader(chunks, self.canaries, policy)
        reader.payloads = []
        rows = stream._tar(reader, policy, self.canaries)
        require(len(rows) == len(reader.payloads), "tar_payload_inventory_mismatch")
        for row, payload in zip(rows, reader.payloads):
            if row["type"] == "file":
                require(row["sha256"] == payload["sha256"] and row["size"] == payload["size"], "tar_payload_mismatch")
                row["scan"] = payload
            self.note({k: v for k, v in row.items() if k != "scan"})
        return rows

    def ar(self, chunks, depth):
        reader = _Input(chunks, self)
        require(reader.exact(8) == b"!<arch>\n", "invalid_ar_magic")
        rows, seen = [], set()
        while True:
            header = reader.read(60)
            if not header:
                break
            require(len(header) == 60 and header[-2:] == b"`\n", "invalid_ar_header")
            name = header[:16].decode("ascii").rstrip(" ").removesuffix("/")
            # Debian package AR names are short, non-nested plain names. Do
            # not accept GNU/BSD long-name indirection or symbol-table offsets.
            require(name and name not in seen and not any(c in name for c in "/\\\x00")
                    and name not in (".", "..") and not name.startswith("#1/"), "unsafe_ar_name")
            seen.add(name)
            size_text = header[48:58].strip()
            require(size_text.isdigit(), "invalid_ar_size")
            size = int(size_text)
            require(size <= self.limits.member_bytes, "member_size_limit")
            for field, base in ((header[16:28], 10), (header[28:34], 10),
                                (header[34:40], 10), (header[40:48], 8)):
                value = field.strip()
                require(value and all(c in (b"01234567" if base == 8 else b"0123456789") for c in value), "invalid_ar_metadata")
            child = self.scan(_bounded(reader, size), depth + 1)
            if size % 2:
                require(reader.exact(1) == b"\n", "invalid_ar_padding")
            row = {"path": name, "type": "file", "size": size, "sha256": child["sha256"], "scan": child}
            self.note({k: v for k, v in row.items() if k != "scan"})
            rows.append(row)
        return rows

    def zip(self, chunks, depth):
        raw = bytearray()
        for chunk in chunks:
            require(len(raw) + len(chunk) <= self.limits.buffered_bytes, "zip_buffer_limit")
            raw.extend(chunk)
        rows, seen = [], set()
        # Bound and walk the central directory BEFORE ZipFile allocates one
        # Python object per entry. ZIP64/multi-disk/digital-signature extensions
        # are deliberately outside this supported wheel/archive subset.
        end = bytes(raw).rfind(b"PK\x05\x06")
        require(end >= 0 and end + 22 <= len(raw), "zip_end_missing")
        comment_size = int.from_bytes(raw[end + 20:end + 22], "little")
        require(end + 22 + comment_size == len(raw), "zip_trailing_data")
        disk, start_disk, disk_count, count, central_size, central_offset = struct.unpack_from("<4H2I", raw, end + 4)
        require(disk == start_disk == 0 and disk_count == count
                and count <= self.limits.entries - self.entries
                and central_size <= self.limits.metadata_bytes
                and central_offset + central_size == end, "unsupported_zip_layout")
        cursor = central_offset
        for _ in range(count):
            require(cursor + 46 <= end and raw[cursor:cursor + 4] == b"PK\x01\x02", "invalid_zip_directory")
            names, extras, comments = struct.unpack_from("<3H", raw, cursor + 28)
            cursor += 46 + names + extras + comments
            require(cursor <= end, "invalid_zip_directory")
        require(cursor == end, "invalid_zip_directory")
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            items = archive.infolist()
            require(len(items) <= self.limits.entries, "zip_entry_limit")
            # Reject prepended/trailing bytes; every supported member is read
            # to EOF, which also checks its CRC. No archive.extract call exists.
            require(count == len(items) and archive.start_dir == central_offset,
                    "unsupported_zip_layout")
            expected_offset = 0
            stream.CanaryScanner(self.canaries).feed(bytes(archive.comment))
            for item in items:
                require(item.header_offset == expected_offset, "zip_unaccounted_or_overlapping_bytes")
                start = item.header_offset
                require(raw[start:start + 4] == b"PK\x03\x04" and start + 30 <= central_offset, "invalid_zip_local_header")
                flags, method = struct.unpack_from("<2H", raw, start + 6)
                name_size, extra_size = struct.unpack_from("<2H", raw, start + 26)
                require(flags == item.flag_bits and method == item.compress_type, "zip_local_metadata_mismatch")
                payload_end = start + 30 + name_size + extra_size + item.compress_size
                require(payload_end <= central_offset, "zip_member_outside_payload")
                expected_offset = payload_end
                if flags & 8:
                    descriptor = expected_offset + (4 if raw[expected_offset:expected_offset + 4] == b"PK\x07\x08" else 0)
                    require(descriptor + 12 <= central_offset, "zip_descriptor_truncated")
                    require(struct.unpack_from("<3I", raw, descriptor) == (item.CRC, item.compress_size, item.file_size),
                            "zip_descriptor_mismatch")
                    expected_offset = descriptor + 12
                else:
                    require(struct.unpack_from("<3I", raw, start + 14) == (item.CRC, item.compress_size, item.file_size),
                            "zip_size_or_crc_mismatch")
                path = stream._path(item.filename.rstrip("/"), stream.Limits(), target=True)
                require(path not in seen, "duplicate_zip_path")
                seen.add(path)
                require(not item.flag_bits & (1 | 64 | 8192), "encrypted_zip")
                # Python 3.9 ZipExtFile bounds DEFLATE output but calls its
                # BZIP2/LZMA decompressors without max_length. Reject those ZIP
                # methods rather than permit an unbounded intermediate buffer.
                require(item.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED), "unsupported_zip_compression")
                require(0 <= item.file_size <= self.limits.member_bytes
                    and 0 <= item.compress_size <= self.limits.member_bytes, "member_size_limit")
                mode = item.external_attr >> 16
                require(not stat.S_IFMT(mode) or stat.S_IFMT(mode) in (stat.S_IFREG, stat.S_IFDIR, stat.S_IFLNK),
                        "unsupported_zip_type")
                for text in (item.filename.encode(), item.comment, item.extra):
                    stream.CanaryScanner(self.canaries).feed(text)
                with archive.open(item) as member:
                    def pieces():
                        count = 0
                        while True:
                            data = member.read(self.limits.chunk_bytes)
                            if not data:
                                break
                            count += len(data)
                            require(count <= item.file_size, "zip_size_changed")
                            yield data
                        require(count == item.file_size, "zip_truncated")
                    child = self.scan(pieces(), depth + 1)
                row = {"path": path, "type": "directory" if item.is_dir() else
                       "symlink" if stat.S_ISLNK(mode) else "file", "size": item.file_size,
                       "mode": mode, "compression": item.compress_type, "sha256": child["sha256"], "scan": child}
                self.note({k: v for k, v in row.items() if k != "scan"})
                rows.append(row)
            require(expected_offset == central_offset, "zip_unaccounted_or_overlapping_bytes")
        return rows


def scan(path, *, canaries, limits=Limits()):
    """Scan a complete stable archive; callers bind its owner and stopped state."""
    require(type(limits) is Limits, "invalid_limits")
    state = _State(canaries, limits)
    policy = stream.Limits(chunk_bytes=limits.chunk_bytes)
    digest = hashlib.sha256()
    try:
        with stream._opened(path) as fd:
            size = os.fstat(fd).st_size
            chunks = stream._chunks(fd, limits.archive_bytes, policy, stream.CanaryScanner(state.canaries), digest)
            result = state.scan(chunks, outer=True)
        require(result["size"] == size and result["sha256"] == digest.hexdigest(), "archive_hash_mismatch")
    except (UnicodeError, tarfile.TarError, zipfile.BadZipFile, zlib.error, lzma.LZMAError,
            EOFError, OSError, OverflowError, RuntimeError):
        raise CacheError("invalid_archive_or_compression") from None
    return {"schema_version": 1, "archive": {"size": size, "sha256": digest.hexdigest()},
        "members": result["members"], "encodings": sorted(state.encodings),
        "decoded_bytes": state.decoded, "canary_count": len(state.canaries), "complete": True,
        "decoded_bytes_accounting": "aggregate_bytes_visited_including_raw_and_nested_levels",
        "scope": "literal_private_canaries_in_stopped_worker_archive_and_supported_decodings",
        "exclusions": ["arbitrary_or_encrypted_encodings", "formats_are_detected_by_signature_not_filename",
                       "deleted_sectors", "live_memory",
                       "ownership_and_capture_quiescence_are_caller_obligations"]}
