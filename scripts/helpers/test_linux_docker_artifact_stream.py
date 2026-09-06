"""Offline, real-byte tests of primitives; no Docker/OCI certification claimed."""
from dataclasses import replace
import gzip
import hashlib
import io
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import linux_docker_artifact_stream as stream


def digest(data):
    return hashlib.sha256(data).hexdigest()


def archive(entries, *, fmt=tarfile.PAX_FORMAT, global_pax=None):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=fmt, pax_headers=global_pax) as writer:
        for entry in entries:
            name, payload, options = entry
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            for key, value in options.items():
                setattr(info, key, value)
            writer.addfile(info, io.BytesIO(payload))
    return output.getvalue()


class ArtifactStreamTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vz-artifact-stream-")
        self.addCleanup(self.temporary.cleanup)
        # macOS /var and /tmp aliases are intentionally rejected by the API.
        self.root = Path(self.temporary.name).resolve()
        self.limits = replace(stream.Limits(), chunk_bytes=7)

    def write(self, data, name="layer"):
        path = self.root / name
        path.write_bytes(data)
        return path

    def layer(self, data, *, compression="uncompressed", **kwargs):
        return stream.scan_layer(self.write(data), compression=compression,
                                 limits=kwargs.pop("limits", self.limits), **kwargs)

    def fails(self, code, callback):
        with self.assertRaises(stream.ArtifactError) as caught:
            callback()
        self.assertEqual(str(caught.exception), code)

    def test_canary_cross_chunk_and_static_error(self):
        scanner = stream.CanaryScanner([b"private-secret"])
        for data in (b"prefixpri", b"vate-", b"secr"):
            scanner.feed(data)
        self.fails("secret_canary_detected", lambda: scanner.feed(b"et-suffix"))

    def test_invalid_canaries_and_limits(self):
        for canaries in ([b""], ["text"], [b"x"] * 65, [b"x" * 65537]):
            with self.subTest(canaries_type=type(canaries[0])):
                self.fails("invalid_canaries", lambda: stream.CanaryScanner(canaries))
        self.fails("invalid_limits", lambda: stream.Limits(entries=True))
        self.fails("invalid_chunk_limit", lambda: stream.Limits(chunk_bytes=1048577))

    def test_canary_iterator_is_bounded(self):
        consumed = []

        def canaries():
            for index in range(10000):
                consumed.append(index)
                yield b"x"

        self.fails("invalid_canaries", lambda: stream.CanaryScanner(canaries()))
        self.assertEqual(len(consumed), 65)

    def test_file_hash_all_bytes_and_empty(self):
        for data in (b"", bytes(range(256)) * 19):
            self.assertEqual(stream.scan_file(self.write(data), limits=self.limits),
                             {"size": len(data), "sha256": digest(data)})

    def test_file_canary_and_size_limit(self):
        path = self.write(b"123456private-secret-tail")
        self.fails("secret_canary_detected", lambda: stream.scan_file(
            path, canaries=[b"private-secret"], limits=self.limits))
        self.fails("source_size_limit", lambda: stream.scan_file(
            path, limits=replace(self.limits, file_bytes=3)))

    def test_host_symlink_ancestor_hardlink_fifo_and_noncanonical(self):
        original = self.write(b"safe")
        link = self.root / "link"
        link.symlink_to(original)
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        for path in (link, alias / "layer"):
            self.fails("unsafe_or_unavailable_source", lambda: stream.scan_file(path))
        hard = self.root / "hard"
        os.link(original, hard)
        self.fails("hardlinked_source", lambda: stream.scan_file(original))
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        self.fails("nonregular_source", lambda: stream.scan_file(fifo))
        for path in ("relative", str(self.root) + "/../layer", str(self.root) + "//layer"):
            self.fails("noncanonical_path", lambda: stream.scan_file(path))

    def test_file_in_place_mutation_detected(self):
        path = self.write(b"abcdefghijk")
        original_read = os.read
        changed = False

        def mutate(descriptor, count):
            nonlocal changed
            data = original_read(descriptor, count)
            if data and not changed:
                changed = True
                path.write_bytes(b"ABCDEFGHIJK")
            return data

        with mock.patch.object(stream.os, "read", side_effect=mutate):
            self.fails("source_changed", lambda: stream.scan_file(path, limits=self.limits))

    def test_file_growth_is_not_partially_certified(self):
        path = self.write(b"abcdefghijk")
        original_read = os.read
        changed = False

        def mutate(descriptor, count):
            nonlocal changed
            data = original_read(descriptor, count)
            if data and not changed:
                changed = True
                with path.open("ab") as writer:
                    writer.write(b"growth")
            return data

        with mock.patch.object(stream.os, "read", side_effect=mutate):
            self.fails("source_size_changed", lambda: stream.scan_file(path, limits=self.limits))

    def test_source_replacement_detected(self):
        path = self.write(b"abcdefghijk")
        replacement = self.write(b"new", "replacement")
        original_read = os.read
        changed = False

        def mutate(descriptor, count):
            nonlocal changed
            data = original_read(descriptor, count)
            if data and not changed:
                changed = True
                path.rename(self.root / "retained-original")
                replacement.rename(path)
            return data

        with mock.patch.object(stream.os, "read", side_effect=mutate):
            with self.assertRaises(stream.ArtifactError):
                stream.scan_file(path, limits=self.limits)

    def test_inventory_complete_sorted_and_no_source_mutation(self):
        self.write(b"a", "z space")
        (self.root / "empty").mkdir()
        (self.root / "sub").mkdir()
        self.write(b"payload", "sub/é")
        result = stream.inventory_tree(self.root, limits=self.limits)
        self.assertEqual([r["path"] for r in result["files"]], ["sub/é", "z space"])
        self.assertEqual([r["path"] for r in result["directories"]], ["empty", "sub"])
        self.assertEqual(result["total_bytes"], 8)
        self.assertEqual(result["files"][0]["sha256"], digest(b"payload"))
        self.assertEqual(result, stream.inventory_tree(self.root, limits=self.limits))

    def test_inventory_final_pass_catches_earlier_file_change(self):
        earlier = self.write(b"aaa", "a")
        later = self.write(b"bbb", "b")
        later_inode = later.stat().st_ino
        original_read = os.read
        changed = False

        def mutate(descriptor, count):
            nonlocal changed
            data = original_read(descriptor, count)
            if data and os.fstat(descriptor).st_ino == later_inode and not changed:
                changed = True
                earlier.write_bytes(b"new")
            return data

        with mock.patch.object(stream.os, "read", side_effect=mutate):
            self.fails("tree_entry_changed", lambda: stream.inventory_tree(self.root, limits=self.limits))

    def test_inventory_limits_names_and_links(self):
        self.write(b"aaa", "a")
        self.write(b"bbb", "b")
        self.fails("tree_entry_limit", lambda: stream.inventory_tree(
            self.root, limits=replace(self.limits, entries=1)))
        self.fails("tree_size_limit", lambda: stream.inventory_tree(
            self.root, limits=replace(self.limits, tree_bytes=5)))
        self.fails("secret_canary_detected", lambda: stream.inventory_tree(self.root, canaries=[b"a"]))
        (self.root / "link").symlink_to(self.root / "a")
        self.fails("nonregular_tree_entry", lambda: stream.inventory_tree(self.root))

    def test_inventory_scandir_stops_at_entry_bound(self):
        calls = []

        class Entries:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def __iter__(self):
                for i in range(100):
                    calls.append(i)
                    yield type("Entry", (), {"name": str(i)})()

        with mock.patch.object(stream.os, "scandir", return_value=Entries()):
            self.fails("tree_entry_limit", lambda: stream.inventory_tree(
                self.root, limits=replace(self.limits, entries=1)))
        self.assertEqual(calls, [0, 1])

    def test_inventory_path_depth_and_hardlink_bounds(self):
        (self.root / "a").mkdir()
        (self.root / "a/b").mkdir()
        self.fails("tree_depth_limit", lambda: stream.inventory_tree(
            self.root, limits=replace(self.limits, depth=1)))
        self.write(b"x", "long-name")
        self.fails("path_size_limit", lambda: stream.inventory_tree(
            self.root, limits=replace(self.limits, path_bytes=5)))
        os.link(self.root / "long-name", self.root / "hard")
        self.fails("hardlinked_source", lambda: stream.inventory_tree(self.root))

    def test_tar_and_gzip_full_hashes_and_file_payload(self):
        raw = archive([("dir", b"", {"type": tarfile.DIRTYPE}),
                       ("dir/value", b"payload" * 100, {"mode": 0o755})])
        plain = self.layer(raw)
        compressed = gzip.compress(raw, mtime=0)
        zipped = self.layer(compressed, compression="gzip")
        self.assertEqual(plain["members"], zipped["members"])
        self.assertEqual(zipped["compressed_sha256"], digest(compressed))
        self.assertEqual(zipped["compressed_size"], len(compressed))
        self.assertEqual(zipped["diff_id"], "sha256:" + digest(raw))
        self.assertEqual(zipped["uncompressed_size"], len(raw))
        self.assertEqual(zipped["members"][1]["sha256"], digest(b"payload" * 100))
        self.assertEqual(zipped["members"][1]["mode"], 0o755)
        self.assertFalse((self.root / "dir").exists())

    def test_tar_legitimate_links_devices_and_normalization(self):
        data = archive([("./root//file", b"x", {}),
                        ("link", b"", {"type": tarfile.SYMTYPE, "linkname": "/root/file"}),
                        ("hard", b"", {"type": tarfile.LNKTYPE, "linkname": "./root/file"}),
                        ("device", b"", {"type": tarfile.CHRTYPE, "devmajor": 1, "devminor": 3})])
        rows = self.layer(data)["members"]
        self.assertEqual(rows[0]["path"], "root/file")
        self.assertEqual(rows[1]["link_target"], "/root/file")
        self.assertEqual(rows[2]["link_target"], "root/file")
        self.assertEqual(rows[3]["device_minor"], 3)

    def test_pax_effective_ownership_and_long_paths(self):
        name = "long/" + "a" * 200
        raw = archive([("raw", b"x", {"uid": 1, "gid": 2,
                        "pax_headers": {"path": name, "uid": "123", "gid": "456"}})])
        row = self.layer(raw)["members"][0]
        self.assertEqual((row["path"], row["uid"], row["gid"]), (name, 123, 456))
        gnu = archive([(name, b"x", {})], fmt=tarfile.GNU_FORMAT)
        self.assertEqual(self.layer(gnu)["members"][0]["path"], name)

    def test_pax_invalid_ownership_sparse_unknown(self):
        for attrs in ({"uid": "-1"}, {"gid": "4294967296"}, {"uid": "١"},
                      {"GNU.sparse.size": "9"}, {"SCHILY.filetype": "sparse"},
                      {"SCHILY.realsize": "9"}, {"future.semantic": "value"}):
            with self.subTest(keys=list(attrs)):
                with self.assertRaises(stream.ArtifactError):
                    self.layer(archive([("file", b"x", {"pax_headers": attrs})]))

    def test_pax_size_ascii_and_dangling_extension(self):
        self.fails("invalid_pax_size", lambda: self.layer(archive([
            ("file", b"x", {"pax_headers": {"size": "١"}})])))
        raw = archive([("file", b"x", {"pax_headers": {"path": "alternate"}})])
        self.fails("invalid_tar_end", lambda: self.layer(raw[:1024] + b"\0" * 1024))
        self.fails("metadata_size_limit", lambda: self.layer(
            raw, limits=replace(self.limits, metadata_bytes=8)))

    def test_aggregate_inherited_pax_metadata_bounded(self):
        raw = archive([(str(i), b"", {}) for i in range(8)], global_pax={"comment": "x" * 100})
        self.fails("reported_metadata_limit", lambda: self.layer(
            raw, limits=replace(self.limits, metadata_bytes=600)))

    def test_canary_in_gzip_header_payload_and_tar_metadata(self):
        canary = b"unique-private-secret"
        for name, payload, options in (("file", canary, {}),
                                       (canary.decode(), b"", {}),
                                       ("file", b"", {"pax_headers": {"comment": canary.decode()}})):
            raw = archive([(name, payload, options)])
            self.fails("secret_canary_detected", lambda: self.layer(
                gzip.compress(raw), compression="gzip", canaries=[canary]))
        output = io.BytesIO()
        with gzip.GzipFile(filename=canary.decode(), mode="wb", fileobj=output, mtime=0) as zipped:
            zipped.write(archive([]))
        self.fails("secret_canary_detected", lambda: self.layer(
            output.getvalue(), compression="gzip", canaries=[canary]))

    def test_canary_assembled_from_ustar_prefix_and_name(self):
        name = "a" * 90 + "/" + "b" * 90
        raw = archive([(name, b"", {})], fmt=tarfile.USTAR_FORMAT)
        self.assertNotIn(b"aaaa/bbbb", raw)
        self.fails("secret_canary_detected", lambda: self.layer(raw, canaries=[b"aaaa/bbbb"]))

    def test_raw_gzip_trailer_scanned_and_source_stable_across_decode(self):
        zipped = gzip.compress(archive([("file", b"payload", {})]), mtime=0)
        self.fails("secret_canary_detected", lambda: self.layer(
            zipped, compression="gzip", canaries=[zipped[-8:]]))
        path = self.write(zipped)
        original_tar = stream._tar

        def mutate(reader, limits, canaries):
            result = original_tar(reader, limits, canaries)
            with path.open("r+b") as writer:
                writer.write(b"XX")
            return result

        with mock.patch.object(stream, "_tar", side_effect=mutate):
            self.fails("source_changed", lambda: stream.scan_layer(path, compression="gzip"))

    def test_paths_hardlinks_and_symlink_traversal_rejected(self):
        for name in ("../escape", "/absolute", "ok/../../escape"):
            self.fails("unsafe_archive_path", lambda: self.layer(archive([(name, b"", {})])))
        for target in ("../escape", "/absolute"):
            self.fails("unsafe_archive_path", lambda: self.layer(archive([
                ("hard", b"", {"type": tarfile.LNKTYPE, "linkname": target})])))
        self.fails("unsafe_archive_hardlink", lambda: self.layer(archive([
            ("hard", b"", {"type": tarfile.LNKTYPE, "linkname": "link"}),
            ("link", b"", {"type": tarfile.SYMTYPE, "linkname": "/outside"})])))
        for extra in (("link/file", b"x", {}),
                      ("hard", b"", {"type": tarfile.LNKTYPE, "linkname": "link/file"})):
            self.fails("archive_symlink_traversal", lambda: self.layer(archive([
                extra, ("link", b"", {"type": tarfile.SYMTYPE, "linkname": "/outside"})])))

    def test_duplicate_sparse_unknown_type_and_limits(self):
        self.fails("duplicate_archive_path", lambda: self.layer(archive([
            ("file", b"", {}), ("./file", b"", {})])))
        for kind in (tarfile.GNUTYPE_SPARSE, b"V"):
            self.fails("unsupported_tar_type", lambda: self.layer(archive([
                ("file", b"", {"type": kind})], fmt=tarfile.GNU_FORMAT)))
        raw = archive([("file", b"payload", {})])
        self.fails("member_size_limit", lambda: self.layer(raw, limits=replace(self.limits, member_bytes=6)))
        self.fails("source_size_limit", lambda: self.layer(raw, limits=replace(self.limits, compressed_bytes=100)))
        self.fails("uncompressed_size_limit", lambda: self.layer(
            gzip.compress(raw), compression="gzip", limits=replace(self.limits, uncompressed_bytes=100)))
        self.fails("tar_entry_limit", lambda: self.layer(
            archive([("a", b"", {}), ("b", b"", {})]), limits=replace(self.limits, entries=1)))

    def test_truncated_corrupt_or_extra_gzip_rejected(self):
        zipped = gzip.compress(archive([("file", b"x", {})]), mtime=0)
        damaged = bytearray(zipped)
        damaged[-8] ^= 1
        for data in (zipped[:-1], zipped[:8], bytes(damaged), zipped + b"junk", zipped + zipped):
            with self.subTest(size=len(data)):
                with self.assertRaises(stream.ArtifactError):
                    self.layer(data, compression="gzip")

    def test_tar_truncation_padding_checksum_trailing_and_unsupported_compression(self):
        raw = archive([("file", b"payload", {})])
        bad_padding = bytearray(raw)
        bad_padding[520] = 1
        bad_checksum = bytearray(raw)
        bad_checksum[0] ^= 1
        for data in (raw[:515], raw[:1024], raw[:-1], raw + b"junk", bytes(bad_padding), bytes(bad_checksum)):
            with self.subTest(size=len(data)):
                with self.assertRaises(stream.ArtifactError):
                    self.layer(data)
        self.fails("unsupported_compression", lambda: self.layer(raw, compression="zstd"))


if __name__ == "__main__":
    unittest.main()
