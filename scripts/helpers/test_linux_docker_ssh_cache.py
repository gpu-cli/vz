"""Bounded generated archives only; no extraction, process, network, or VM."""
import bz2
from dataclasses import replace
import gzip
import hashlib
import io
import json
import lzma
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
import zipfile

import linux_docker_artifact_stream as stream
import linux_docker_ssh_cache as cache

CANARY = b"private-disposable-key-material-abcdef0123456789"


def tar(items):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name, value in items:
            item = tarfile.TarInfo(name)
            if isinstance(value, bytes):
                item.size = len(value)
                archive.addfile(item, io.BytesIO(value))
            else:
                item.type, item.linkname = value
                archive.addfile(item)
    return output.getvalue()


def ar(items):
    result = bytearray(b"!<arch>\n")
    for name, data in items:
        header = ("%-16s%-12d%-6d%-6d%-8s%-10d`\n" % (name + "/", 0, 0, 0, "100644", len(data))).encode()
        assert len(header) == 60
        result.extend(header + data + (b"\n" if len(data) % 2 else b""))
    return bytes(result)


def zip_bytes(items, compression=zipfile.ZIP_DEFLATED):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        for name, data in items:
            archive.writestr(name, data)
    return output.getvalue()


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-cache-scan-", dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "worker.tar"

    def scan(self, data, **kwargs):
        self.path.write_bytes(data)
        return cache.scan(self.path, canaries=(CANARY,), **kwargs)

    def rejects(self, data, code=None, **kwargs):
        with self.assertRaises((cache.CacheError, stream.ArtifactError)) as error:
            self.scan(data, **kwargs)
        if code:
            self.assertIn(code, str(error.exception))
        self.assertNotIn(CANARY.decode(), str(error.exception))

    def test_complete_raw_archive_inert_links_and_exact_hashes(self):
        data = tar([("worker/file", b"public"), ("worker/alias", (tarfile.SYMTYPE, "/outside/never-follow")),
                    ("worker/hard", (tarfile.LNKTYPE, "worker/file"))])
        proof = self.scan(data)
        self.assertTrue(proof["complete"])
        self.assertEqual(proof["archive"], {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
        self.assertEqual(proof["members"][0]["scan"]["sha256"], hashlib.sha256(b"public").hexdigest())
        self.assertEqual(proof["members"][1]["link_target"], "/outside/never-follow")
        self.assertEqual(list(Path(self.temp.name).iterdir()), [self.path])
        self.assertNotIn(CANARY.decode(), json.dumps(proof))

    def test_nested_buildkit_blob_debian_ar_xz_and_gzip(self):
        package = ar([("debian-binary", b"2.0\n"),
                      ("control.tar.gz", gzip.compress(tar([("control", b"Package: disposable\n")]))),
                      ("data.tar.xz", lzma.compress(tar([("usr/bin/ssh", b"public executable")])) )])
        blob = gzip.compress(tar([("fixture/packages/client.deb", package)]))
        proof = self.scan(tar([("content/blobs/sha256/abc", blob)]))
        self.assertEqual(proof["encodings"], ["ar", "gzip", "raw", "tar", "xz"])

    def test_recursive_canary_inside_debian_payload_rejected(self):
        package = ar([("data.tar.xz", lzma.compress(tar([("leak", CANARY)])))])
        self.rejects(tar([("blob", gzip.compress(tar([("client.deb", package)])))]), "secret_canary_detected")

    def test_bzip2_and_bounded_wheel_zip_supported(self):
        wheel = zip_bytes([("pkg/__init__.py", b"public"), ("pkg/docs.txt.bz2", bz2.compress(b"text"))])
        proof = self.scan(tar([("ensurepip/tool.whl", wheel)]))
        self.assertIn("zip", proof["encodings"])
        self.assertIn("bzip2", proof["encodings"])

    def test_canary_in_compressed_wheel_member(self):
        self.rejects(tar([("tool.whl", zip_bytes([("payload", CANARY)]))]), "secret_canary_detected")

    def test_canary_crosses_raw_and_decoded_chunk_boundaries(self):
        for data in (b"x" * 29 + CANARY, gzip.compress(b"x" * 29 + CANARY)):
            with self.subTest(kind=data[:2]):
                self.rejects(tar([("payload", data)]), "secret_canary_detected",
                             limits=replace(cache.Limits(), chunk_bytes=32))

    def test_metadata_canary_rejected(self):
        for rows in ([(CANARY.decode(), b"public")], [("alias", (tarfile.SYMTYPE, CANARY.decode()))]):
            with self.subTest(rows=rows):
                self.rejects(tar(rows), "secret_canary_detected")

    def test_truncated_and_concatenated_compression_rejected(self):
        for encode in (gzip.compress, lzma.compress, bz2.compress):
            good = encode(b"public")
            for bad in (good[:-4], good + b"junk", good + good):
                with self.subTest(kind=encode.__module__, size=len(bad)):
                    self.rejects(tar([("blob", bad)]))

    def test_zstd_and_unsupported_compression_never_treated_as_raw(self):
        for magic in (b"\x28\xb5\x2f\xfd", b"\x50\x2a\x4d\x18", b"\x04\x22\x4d\x18",
                      b"7z\xbc\xaf\x27\x1c", b"Rar!", b"LZIP"):
            with self.subTest(magic=magic): self.rejects(tar([("blob", magic + b"opaque")]))

    def test_filename_is_not_an_encoding_declaration(self):
        # Real pinned Debian base stores a plain alternatives database under
        # this name; treating all .gz filenames as gzip rejects valid input.
        proof = self.scan(tar([("var/lib/dpkg/alternatives/builtins.7.gz", b"auto\n/usr/share/man/man7/builtins.7.gz\n")]))
        self.assertEqual(proof["members"][0]["scan"]["encoding"], "raw")
        self.assertIn("formats_are_detected_by_signature_not_filename", proof["exclusions"])

    def test_limits_fail_closed(self):
        data = tar([("blob", gzip.compress(b"x" * 100000))])
        for limit in (replace(cache.Limits(), archive_bytes=100),
                      replace(cache.Limits(), decoded_bytes=20000),
                      replace(cache.Limits(), member_bytes=8),
                      replace(cache.Limits(), depth=1),
                      replace(cache.Limits(), metadata_bytes=8)):
            with self.subTest(limit=limit): self.rejects(data, limits=limit)
        self.rejects(tar([("a", b"a"), ("b", b"b")]), limits=replace(cache.Limits(), entries=1))
        self.rejects(tar([("a.zip", zip_bytes([("a", b"a")]))]), limits=replace(cache.Limits(), buffered_bytes=10))

    def test_duplicate_and_traversing_tar_paths_rejected(self):
        for rows in ([("../foreign", b"x")], [("same", b"a"), ("same", b"b")],
                     [("dir", (tarfile.SYMTYPE, "elsewhere")), ("dir/file", b"x")]):
            with self.subTest(rows=rows): self.rejects(tar(rows))

    def test_bad_ar_indirection_and_lengths_rejected(self):
        for name in ("../x", "#1/8", "/", "same"):
            rows = [(name, b"abc")]
            if name == "same": rows.append((name, b"def"))
            self.rejects(tar([("bad.deb", ar(rows))]))
        self.rejects(tar([("bad.deb", ar([("data.tar.xz", lzma.compress(b"public"))])[:-2])]))

    def test_zip_encryption_unknown_methods_and_unsafe_names_rejected(self):
        for name in ("../outside", "/absolute"):
            self.rejects(tar([("a.zip", zip_bytes([(name, b"x")]))]))
        self.rejects(tar([("a.zip", zip_bytes([("a", b"x")], zipfile.ZIP_LZMA))]), "unsupported_zip_compression")
        good = bytearray(zip_bytes([("a", b"x")]))
        central = good.index(b"PK\x01\x02")
        good[6:8] = (1).to_bytes(2, "little")
        good[central + 8:central + 10] = (1).to_bytes(2, "little")
        self.rejects(tar([("a.zip", bytes(good))]), "encrypted_zip")

    def test_zip_crc_truncation_and_extra_bytes_rejected(self):
        good = zip_bytes([("a", b"public")], zipfile.ZIP_STORED)
        bad_crc = bytearray(good)
        bad_crc[31] ^= 1
        for bad in (good[:-5], good + b"trailer", b"prefix" + good, bytes(bad_crc)):
            with self.subTest(size=len(bad)): self.rejects(tar([("a.zip", bad)]))

    def test_empty_zip_and_data_descriptor_zip_supported(self):
        self.scan(tar([("empty.zip", zip_bytes([]))]))
        class NonSeekable(io.BytesIO):
            def seek(self, *args):
                raise io.UnsupportedOperation()
        output = NonSeekable()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("public", b"bounded descriptor content")
        proof = self.scan(tar([("stream.zip", output.getvalue())]))
        self.assertIn("zip", proof["encodings"])

    def test_zip_directory_counts_checked_before_zipfile_allocation(self):
        raw = bytearray(zip_bytes([("public", b"x")]))
        end = raw.rfind(b"PK\x05\x06")
        struct.pack_into("<2H", raw, end + 8, 0, 0)
        self.rejects(tar([("forged.zip", bytes(raw))]), "invalid_zip_directory")

    def test_zip_unlisted_local_payload_cannot_hide_compressed_data(self):
        good = bytearray(zip_bytes([("a", b"public")]))
        central = good.index(b"PK\x01\x02")
        hidden = gzip.compress(CANARY)
        end = good.rfind(b"PK\x05\x06")
        struct.pack_into("<I", good, end + 16, central + len(hidden))
        good[central:central] = hidden
        self.rejects(tar([("a.zip", bytes(good))]), "zip_unaccounted_or_overlapping_bytes")

    def test_non_tar_empty_canaries_and_host_symlinks_rejected(self):
        self.rejects(b"not tar", "outer_tar_required")
        self.path.write_bytes(tar([("a", b"a")]))
        with self.assertRaises(cache.CacheError): cache.scan(self.path, canaries=())
        link = self.path.parent / "link"
        link.symlink_to(self.path)
        with self.assertRaises(stream.ArtifactError): cache.scan(link, canaries=(CANARY,))


if __name__ == "__main__":
    unittest.main()
