"""Offline packaging tests; no Linux binary, Docker command or VM is executed."""

import importlib.util
import io
import json
import os
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("developer_probe", Path(__file__).with_name("developer-probe.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def elf():
    data = bytearray(256)
    data[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<HH", data, 16, 2, 183)
    struct.pack_into("<Q", data, 32, 64)
    struct.pack_into("<HH", data, 54, 56, 1)
    struct.pack_into("<I", data, 64, 1)
    return bytes(data)


class ProbePackagingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / "busybox"
        self.binary.write_bytes(elf())
        self.provenance = self.root / "busybox.build.json"
        self.data = {"schema_version": 1, "artifact_sha256": probe.sha(elf()),
                     "build_parameters": {"kind": "busybox", "arch": "arm64"},
                     "source": {"archive_sha256": "a" * 64, "source_tree_sha256": "b" * 64,
                                "archive_root": "busybox-1.37.0", "case_sensitive_storage": True}}
        self.write_provenance()

    def write_provenance(self):
        self.provenance.write_text(json.dumps(self.data))

    def create(self):
        return probe.create(self.binary, self.provenance, "a" * 64, "1.37.0")

    def test_deterministic_bytes_ignore_host_mode_and_timestamp(self):
        first = self.create()
        self.binary.chmod(0o600)
        os.utime(self.binary, (123, 456))
        self.assertEqual(first, self.create())

    def test_archive_is_exact_rootfs_without_runtime_or_external_paths(self):
        archive, metadata = self.create()
        self.assertEqual(metadata["sha256"], probe.sha(archive))
        self.assertEqual(metadata["build_provenance_sha256"], probe.sha(self.provenance.read_bytes()))
        with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
            members = stream.getmembers()
            self.assertEqual([m.name for m in members], sorted(["bin", "bin/busybox", "etc", "etc/vz-developer-probe", "tmp"] +
                                                              ["bin/" + name for name in probe.APPLETS]))
            self.assertEqual(stream.extractfile("bin/busybox").read(), elf())
            self.assertEqual(stream.extractfile("etc/vz-developer-probe").read(), probe.MARKER)
            for member in members:
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, 0))
                if member.issym():
                    self.assertEqual(member.linkname, "busybox")
                self.assertFalse(member.name.startswith("/"))
                self.assertNotIn("..", member.name.split("/"))

    def test_poisoned_binary_source_or_version_rejected(self):
        self.binary.write_bytes(elf() + b"changed")
        with self.assertRaises(ValueError):
            self.create()
        self.binary.write_bytes(elf())
        self.data["source"]["archive_sha256"] = "c" * 64
        self.write_provenance()
        with self.assertRaises(ValueError):
            self.create()
        self.data["source"]["archive_sha256"] = "a" * 64
        self.data["source"]["archive_root"] = "busybox-9.9.9"
        self.write_provenance()
        with self.assertRaises(ValueError):
            self.create()

    def test_dynamic_interpreter_wrong_arch_and_bad_headers_rejected(self):
        for offset, fmt, value in ((16, "H", 3), (18, "H", 62), (64, "I", 2), (64, "I", 3), (54, "H", 1), (56, "H", 129)):
            binary = bytearray(elf())
            struct.pack_into("<" + fmt, binary, offset, value)
            with self.subTest(offset=offset, value=value), self.assertRaises(ValueError):
                probe.rootfs_bytes(bytes(binary))

    def test_symlink_hardlink_empty_and_oversize_inputs_rejected(self):
        link = self.root / "link"
        link.symlink_to(self.binary)
        with self.assertRaises(OSError):
            probe.read_regular(link, 1024)
        hardlink = self.root / "hardlink"
        os.link(self.binary, hardlink)
        with self.assertRaises(ValueError):
            self.create()
        hardlink.unlink()
        with self.assertRaises(ValueError):
            probe.read_regular(self.binary, 1)
        self.binary.write_bytes(b"")
        with self.assertRaises(ValueError):
            self.create()

    def test_changed_marker_changes_archive_digest(self):
        archive, _ = self.create()
        original = probe.MARKER
        try:
            probe.MARKER = b"changed-marker\n"
            changed, _ = self.create()
        finally:
            probe.MARKER = original
        self.assertNotEqual(probe.sha(archive), probe.sha(changed))

    def test_publication_refuses_foreign_symlink(self):
        output = self.root / probe.ARCHIVE
        output.symlink_to(self.binary)
        with self.assertRaises(OSError):
            probe.publish(output, b"new")
        self.assertEqual(self.binary.read_bytes(), elf())


if __name__ == "__main__":
    unittest.main()
