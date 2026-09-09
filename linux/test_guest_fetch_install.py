"""The Developer HTTPS client's staging and boot-time install, without a boot.

`init` copies `vz-guest-fetch` across the overlay/chroot boundary, because a
binary left in the initramfs is not a capability any Machine has: `vz exec` runs
inside the chroot. That copy is otherwise only exercised by booting a Machine,
which no offline suite can do, so the function is extracted and run here against
a real temporary root.

This is packaging evidence, not TLS evidence. What the client does once it is
installed is proved by its own tests and by the criterion-6 lane check.
"""
from __future__ import annotations

from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


LINUX = Path(__file__).resolve().parent
FUNCTION = "install_developer_https_client"


class GuestFetchInstallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-guest-fetch-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.origin = self.root / "initramfs-usr-bin-vz-guest-fetch"
        self.origin.write_bytes(b"\x7fELF pretend client\n")
        self.origin.chmod(0o755)
        self.target_root = self.root / "rootfs"
        self.target_root.mkdir()

    def install(self, root: Path = None):
        """Run the real function out of `init` against a temporary root.

        Only two things are relocated: the origin path, and BusyBox applet
        dispatch to the host's own tools. Every rule the function enforces --
        the symlink refusals, the byte comparison, the mode -- is the shipped
        text.
        """
        init = (LINUX / "initramfs/init").read_text()
        body = FUNCTION + " () {" if FUNCTION + " () {" in init else FUNCTION + "() {"
        script = body + init.split(body, 1)[1].split("\n}\n", 1)[0] + "\n}\n"
        script = script.replace(
            "  hc_origin=/usr/bin/vz-guest-fetch\n",
            "  hc_origin=" + shlex.quote(str(self.origin)) + "\n")
        script = script.replace("/bin/busybox ", "")
        target = self.target_root if root is None else root
        return subprocess.run(
            ["/bin/sh", "-c", script + "\n" + FUNCTION + " " + shlex.quote(str(target))],
            env={"PATH": "/usr/bin:/bin"}, capture_output=True, timeout=20, check=False)

    def installed(self) -> Path:
        return self.target_root / "usr/local/bin/vz-guest-fetch"

    def test_the_client_lands_in_the_running_root_executable_and_byte_identical(self):
        self.assertEqual(self.install().returncode, 0)
        landed = self.installed()
        self.assertTrue(landed.is_file() and not landed.is_symlink())
        self.assertEqual(landed.read_bytes(), self.origin.read_bytes())
        # Executable for every Machine user, writable by none: `vz exec` runs it
        # and nothing in the Machine should be able to replace it in place.
        self.assertEqual(landed.stat().st_mode & 0o777, 0o555)

    def test_installing_twice_is_the_same_root_and_not_a_failure(self):
        """A Machine that reboots into the same overlay must not fail closed."""
        self.assertEqual(self.install().returncode, 0)
        first = self.installed().read_bytes()
        self.assertEqual(self.install().returncode, 0)
        self.assertEqual(self.installed().read_bytes(), first)

    def test_a_different_client_already_at_that_path_is_refused(self):
        """The image does not get to substitute its own client for this one.

        An external rootfs may legitimately carry a file at this path. It is
        compared, never overwritten, so what a Machine runs is the binary this
        initramfs verified or the boot fails.
        """
        landed = self.installed()
        landed.parent.mkdir(parents=True)
        landed.write_bytes(b"not the client")
        self.assertNotEqual(self.install().returncode, 0)
        self.assertEqual(landed.read_bytes(), b"not the client")

    def test_a_redirected_path_is_refused_rather_than_followed(self):
        for relative in ("usr/local/bin", "usr/local/bin/vz-guest-fetch"):
            with self.subTest(relative=relative):
                self.setUp()
                elsewhere = self.root / "elsewhere"
                elsewhere.mkdir()
                (elsewhere / "vz-guest-fetch").write_bytes(b"decoy")
                redirected = self.target_root / relative
                redirected.parent.mkdir(parents=True, exist_ok=True)
                redirected.symlink_to(elsewhere if relative.endswith("bin") else elsewhere / "vz-guest-fetch")
                self.assertNotEqual(self.install().returncode, 0)
                self.assertEqual((elsewhere / "vz-guest-fetch").read_bytes(), b"decoy")

    def test_a_missing_origin_is_refused_rather_than_silently_skipped(self):
        """No client is a Machine that cannot fetch; it must not look like success."""
        self.origin.unlink()
        self.assertNotEqual(self.install().returncode, 0)

    def test_the_makefile_stages_it_for_developer_images_only(self):
        make = (LINUX / "Makefile").read_text()
        inputs = make.split("ifeq ($(KERNEL_PROFILE),developer)\nINITRAMFS_PROFILE_INPUTS :=", 1)[1]
        self.assertIn("guest-fetch", inputs.split("else", 1)[0])
        assembly = make.split("ifeq ($(KERNEL_PROFILE),developer)\n\tpython3", 1)[1].split("endif", 1)[0]
        self.assertIn('cp "$(GUEST_FETCH_BINARY)" "$(INITRAMFS_ROOT)/usr/bin/vz-guest-fetch"', assembly)
        # The hardened profile ships no trust bundle, so a client there would
        # have nothing to verify against.
        self.assertNotIn("vz-guest-fetch", make.split("INITRAMFS_PROFILE_INPUTS :=", 2)[2].split("endif", 1)[0])
        # Derived from the agent's path: the Docker builder overrides
        # GUEST_AGENT_BINARY because its CARGO_TARGET_DIR lives in the build
        # volume, and a second path spelled out against the checkout would not
        # exist there.
        self.assertIn("GUEST_FETCH_BINARY = $(dir $(GUEST_AGENT_BINARY))vz-guest-fetch", make)
        # The kernel's empty CROSS_COMPILE reaches every recipe and cc-rs turns
        # it into the tool name `-gcc`; ring compiles C, so this build is the
        # first that would notice.
        self.assertEqual(make.count("env -u CROSS_COMPILE cargo"), 2)

    def test_a_failed_install_ends_the_boot_rather_than_continuing(self):
        init = (LINUX / "initramfs/init").read_text()
        marker = "Developer overlay HTTPS client installation failed"
        self.assertIn("exit 1", init.split(marker, 1)[1].split("fi", 1)[0])
        # Installed before the guest agent starts, because `vz exec` runs
        # through that agent: a binary staged afterwards would be missing for
        # exactly the first commands anyone runs.
        self.assertLess(init.index(FUNCTION + ' "$ROOTFS"'), init.index("starting guest agent on vsock"))
        result = subprocess.run(["/bin/sh", "-n", str(LINUX / "initramfs/init")],
                                capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
