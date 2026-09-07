#!/usr/bin/env python3
"""Small archive fixtures exercise normalization and unsafe-link rejection."""
import importlib.util
import os
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "toolchain", Path(__file__).with_name("prepare-native-swift-toolchain.py"))
toolchain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(toolchain)


class ToolchainArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "input"
        for directory in ["usr/bin", "Library", "SDKs/MacOSX26.1.sdk"]:
            (self.source / directory).mkdir(parents=True)
        (self.source / "usr/bin/swift-frontend").write_bytes(b"compiler fixture")
        (self.source / "usr/bin/swift").symlink_to("swift-frontend")
        (self.source / "SDKs/MacOSX.sdk").symlink_to("MacOSX26.1.sdk")
        (self.source / "SDKs/MacOSX26.1.sdk/SDKSettings.json").write_text('{}\n')
        (self.source / "SDKs/MacOSX26.2.sdk").mkdir()
        (self.source / "SDKs/MacOSX26.2.sdk/excluded").write_text("another SDK")

    def write(self, name):
        destination = self.root / name
        toolchain.write_archive(self.source, destination, "SDKs/MacOSX26.1.sdk")
        return destination

    def test_reproducible_archive_ignores_mtime_and_preserves_internal_links(self):
        first = self.write("first.tar.gz")
        for path in self.source.rglob("*"):
            os.utime(path, (123456789, 123456789), follow_symlinks=False)
        second = self.write("second.tar.gz")
        self.assertEqual(first.read_bytes(), second.read_bytes())
        with tarfile.open(first) as archive:
            members = archive.getmembers()
            self.assertTrue(all(m.uid == m.gid == m.mtime == 0 for m in members))
            self.assertTrue(all("26.2" not in m.name for m in members))
            self.assertEqual(archive.getmember("CommandLineTools/usr/bin/swift").linkname,
                             "swift-frontend")

    def test_external_and_absolute_symlinks_fail_closed(self):
        link = self.source / "usr/bin/escape"
        for target in ["../../../outside", str(self.source / "usr/bin/swift-frontend")]:
            with self.subTest(target=target):
                link.symlink_to(target)
                with self.assertRaisesRegex(AssertionError, "toolchain symlink"):
                    self.write("rejected-" + str(len(target)) + ".tar.gz")
                link.unlink()

    def test_xcode_layout_preserves_the_complete_application_root(self):
        source = self.root / "Xcode.app"
        developer = source / "Contents/Developer"
        developer.mkdir(parents=True)
        (source / "Contents/Info.plist").write_text("app identity")
        (developer / "compiler").write_bytes(b"compiler")
        (developer / "alias").symlink_to("compiler")
        destination = self.root / "xcode.tar.gz"
        toolchain.write_archive(source, destination, "unused SDK path", layout="xcode")
        with tarfile.open(destination) as archive:
            self.assertIn("Xcode.app/Contents/Info.plist", archive.getnames())
            self.assertEqual(archive.getmember("Xcode.app/Contents/Developer/alias").linkname, "compiler")

    def test_only_obsolete_crashlog_alias_is_omitted(self):
        (self.source / "usr/bin/crashlog").symlink_to("../../../SharedFrameworks/missing")
        with tarfile.open(self.write("without-crashlog.tar.gz")) as archive:
            self.assertNotIn("CommandLineTools/usr/bin/crashlog", archive.getnames())
            self.assertIn("CommandLineTools/usr/bin/swift", archive.getnames())


if __name__ == "__main__":
    unittest.main()
