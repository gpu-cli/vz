"""Offline tool provenance tests; these are not Machine E2E evidence."""
import importlib.util
import json
import hashlib
import os
from pathlib import Path
import shlex
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("e2fsprogs_build", Path(__file__).with_name("e2fsprogs-build.py"))
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class ToolAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vz-ext4-build-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def elf(self, name="mke2fs", kind=1, machine=183):
        header = bytearray(120)
        header[:7] = b"\x7fELF\x02\x01\x01"
        struct.pack_into("<HH", header, 16, 2, machine)
        struct.pack_into("<Q", header, 32, 64)
        struct.pack_into("<HH", header, 54, 56, 1)
        struct.pack_into("<I", header, 64, kind)
        path = self.root / name
        path.write_bytes(header)
        path.chmod(0o755)
        return path

    def manifest(self):
        for name in BUILD.TOOLS:
            self.elf(name)
        value = {"schema_version": 1, "version": BUILD.VERSION,
                 "source_sha256": BUILD.SOURCE_SHA256, "recipe_sha256": "recipe",
                 "binaries": {name: BUILD.sha256(self.root / name) for name in BUILD.TOOLS}}
        path = self.root / "e2fsprogs.build.json"
        path.write_text(json.dumps(value))
        return path, value

    def test_static_arm64_executable_is_accepted(self):
        BUILD.verify_elf(self.elf())

    def test_dynamic_foreign_and_truncated_elf_rejected(self):
        for kind, machine in [(2, 183), (3, 183), (1, 62), (0, 183)]:
            with self.assertRaises(ValueError):
                BUILD.verify_elf(self.elf(kind=kind, machine=machine))
        path = self.elf()
        path.write_bytes(path.read_bytes()[:-1])
        with self.assertRaises(ValueError):
            BUILD.verify_elf(path)

    def test_symlink_or_nonexecutable_rejected(self):
        path = self.elf()
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(ValueError):
            BUILD.verify_elf(link)
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            BUILD.verify_elf(path)

    def test_cache_requires_exact_source_recipe_and_binaries(self):
        manifest, value = self.manifest()
        self.assertTrue(BUILD.cached(self.root, "recipe"))
        self.assertFalse(BUILD.cached(self.root, "different-recipe"))
        for key, replacement in [("source_sha256", "0" * 64), ("version", "0.0.0"),
                                 ("schema_version", 2), ("binaries", {})]:
            manifest.write_text(json.dumps(dict(value, **{key: replacement})))
            self.assertFalse(BUILD.cached(self.root, "recipe"))
        manifest.write_text(json.dumps(value))
        binary = self.root / "mke2fs"
        binary.write_bytes(binary.read_bytes() + b"corruption")
        self.assertFalse(BUILD.cached(self.root, "recipe"))

    def test_incomplete_or_symlink_manifest_rejected(self):
        manifest, _ = self.manifest()
        manifest.write_text("{")
        self.assertFalse(BUILD.cached(self.root, "recipe"))
        manifest.unlink()
        manifest.symlink_to(self.root / "mke2fs")
        self.assertFalse(BUILD.cached(self.root, "recipe"))


class OverlayInstallationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vz-ext4-overlay-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        (self.source / "sbin").mkdir(parents=True)
        (self.source / "etc").mkdir()
        binaries = {}
        for name in BUILD.TOOLS:
            payload = ("pinned fixture " + name).encode()
            target = self.source / "sbin" / name
            target.write_bytes(payload)
            target.chmod(0o755)
            binaries[name] = hashlib.sha256(payload).hexdigest()
        self.provenance = self.source / "etc/vz-e2fsprogs.json"
        self.provenance.write_text(json.dumps({"binaries": binaries}, sort_keys=True, indent=2) + "\n")
        self.overlay = self.root / "overlay"
        self.overlay.mkdir()

    def boot(self, install=True):
        # Execute the actual boot functions, relocating only their fixed source
        # root and using host equivalents of BusyBox applets. Not VM evidence.
        init = Path(__file__).with_name("initramfs").joinpath("init").read_text()
        script = "verify_developer_e2fsprogs() {" + init.split("verify_developer_e2fsprogs() {", 1)[1].split("# Developer trust", 1)[0]
        script = script.replace("  e2_origin=/\n", "  e2_origin=" + shlex.quote(str(self.source)) + "\n")
        script = script.replace("/bin/busybox", "bb")
        stat_command = "/usr/bin/stat -f %l" if sys.platform == "darwin" else "/usr/bin/stat -c %h"
        shim = 'bb() { case "$1" in stat) ' + stat_command + ' "$4" ;; sha256sum) /usr/bin/shasum -a 256 "$2" ;; *) command "$@" ;; esac; }\n'
        call = ("install_developer_e2fsprogs " + shlex.quote(str(self.overlay)) if install
                else "verify_developer_e2fsprogs " + shlex.quote(str(self.source)))
        return subprocess.run(["/bin/sh", "-c", shim + script + "\n" + call],
                              capture_output=True, timeout=10, check=False)

    def test_verified_payload_and_provenance_survive_overlay(self):
        self.assertEqual(self.boot(False).returncode, 0)
        result = self.boot()
        self.assertEqual(result.returncode, 0, result.stderr)
        for path in ("sbin/mke2fs", "sbin/dumpe2fs", "etc/vz-e2fsprogs.json"):
            self.assertEqual((self.overlay / path).read_bytes(), (self.source / path).read_bytes())
        self.assertTrue(os.access(self.overlay / "sbin/mke2fs", os.X_OK))
        self.assertEqual(self.boot().returncode, 0)

    def test_missing_tampered_and_nonexecutable_source_refused(self):
        path = self.source / "sbin/mke2fs"
        payload = path.read_bytes()
        path.write_bytes(payload + b"tampered")
        self.assertNotEqual(self.boot().returncode, 0)
        path.write_bytes(payload)
        path.chmod(0o644)
        self.assertNotEqual(self.boot().returncode, 0)
        path.unlink()
        self.assertNotEqual(self.boot().returncode, 0)
        self.assertEqual(list(self.overlay.iterdir()), [])

    def test_duplicate_malformed_or_missing_provenance_refused(self):
        original = self.provenance.read_text()
        field = next(line for line in original.splitlines() if '"mke2fs"' in line)
        for contents in ("{}", original + field + "\n", original.replace('"mke2fs"', '"other"')):
            self.provenance.write_text(contents)
            self.assertNotEqual(self.boot().returncode, 0)
        self.provenance.unlink()
        self.assertNotEqual(self.boot().returncode, 0)

    def test_source_symlink_and_hardlink_refused(self):
        original = self.source / "sbin/mke2fs"
        foreign = self.root / "foreign"
        original.rename(foreign)
        original.symlink_to(foreign)
        self.assertNotEqual(self.boot().returncode, 0)
        original.unlink()
        os.link(foreign, original)
        self.assertNotEqual(self.boot().returncode, 0)

    def test_target_redirection_or_conflicting_bytes_refused(self):
        foreign = self.root / "foreign"
        foreign.mkdir()
        (self.overlay / "sbin").symlink_to(foreign)
        self.assertNotEqual(self.boot().returncode, 0)
        self.assertEqual(list(foreign.iterdir()), [])
        (self.overlay / "sbin").unlink()
        (self.overlay / "sbin").mkdir()
        target = self.overlay / "sbin/mke2fs"
        target.write_bytes(b"foreign formatter")
        self.assertNotEqual(self.boot().returncode, 0)
        self.assertEqual(target.read_bytes(), b"foreign formatter")

    def test_boot_integrity_failure_is_terminal_before_agent_chroot(self):
        init = Path(__file__).with_name("initramfs").joinpath("init").read_text()
        for marker in ("Developer ext4 tooling integrity failed", "Developer overlay ext4 tooling installation failed"):
            self.assertIn("exit 1", init.split(marker, 1)[1].split("fi", 1)[0])
        self.assertLess(init.index('install_developer_e2fsprogs "$ROOTFS"'), init.index("starting guest agent on vsock"))


def linux_overlay_smoke(tools):
    """Exercise real pinned BusyBox/tools in a builder-private chroot, not a VM."""
    if sys.platform != "linux" or not Path("/bin/busybox").is_file():
        raise RuntimeError("smoke requires the explicit Linux builder and pinned /bin/busybox")
    with tempfile.TemporaryDirectory(prefix="vz-ext4-real-overlay-") as temporary:
        source = Path(temporary) / "source"
        overlay = Path(temporary) / "overlay"
        (source / "sbin").mkdir(parents=True)
        (source / "etc").mkdir()
        overlay.mkdir()
        for name in BUILD.TOOLS:
            BUILD.verify_elf(tools / name)
            shutil.copy2(tools / name, source / "sbin" / name)
        shutil.copy2(tools / "e2fsprogs.build.json", source / "etc/vz-e2fsprogs.json")
        init = Path(__file__).with_name("initramfs").joinpath("init").read_text()
        script = "verify_developer_e2fsprogs() {" + init.split("verify_developer_e2fsprogs() {", 1)[1].split("# Developer trust", 1)[0]
        script = script.replace("  e2_origin=/\n", "  e2_origin=" + shlex.quote(str(source)) + "\n")
        subprocess.run(["/bin/busybox", "sh", "-c", script + "\ninstall_developer_e2fsprogs " + shlex.quote(str(overlay))], check=True)
        for name in BUILD.TOOLS:
            subprocess.run(["chroot", str(overlay), f"/sbin/{name}", "-V"], check=True)
            print(f"verified and executed exact overlay /sbin/{name}: {BUILD.sha256(overlay / 'sbin' / name)}", flush=True)
        assert (overlay / "etc/vz-e2fsprogs.json").read_bytes() == (tools / "e2fsprogs.build.json").read_bytes()
        print("real BusyBox overlay installation and chroot execution PASS", flush=True)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--linux-overlay-smoke":
        linux_overlay_smoke(Path(sys.argv[2]))
    else:
        unittest.main()
