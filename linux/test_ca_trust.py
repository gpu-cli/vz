"""Offline public-CA assembly and relocated boot-script tests; no VM/network."""

import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import ssl
import stat
import subprocess
import sys
import tempfile
import unittest


LINUX = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("ca_trust", LINUX / "ca-trust.py")
CA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CA)


class CaTrustTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-ca-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "source"
        shutil.copytree(LINUX / "ca-trust", self.source)
        self.guest = self.root / "initramfs"
        self.guest.mkdir()

    def boot(self, target=None):
        # Only relocate the fixed source root and dispatch BusyBox applets to
        # local equivalent tools. This is not Linux boot/registry TLS evidence.
        script = (self.source / "install.sh").read_text()
        script = script.replace("  ca_origin=/\n", "  ca_origin=" + shlex.quote(str(self.guest)) + "\n")
        script = script.replace("/bin/busybox", "bb")
        stat_command = "/usr/bin/stat -f %l" if sys.platform == "darwin" else "/usr/bin/stat -c %h"
        shim = 'bb() { case "$1" in stat) ' + stat_command + ' "$4" ;; sha256sum) /usr/bin/shasum -a 256 "$2" ;; *) command "$@" ;; esac; }\n'
        call = "verify_developer_ca_trust" if target is None else "install_developer_ca_trust " + shlex.quote(str(target))
        return subprocess.run(["/bin/sh", "-c", shim + script + "\n" + call],
                              env={"PATH": "/usr/bin:/bin"}, capture_output=True, timeout=10, check=False)

    def test_real_vendored_public_bundle_exact_hash_and_certificate_inventory(self):
        pin, bundle, _, _ = CA.verify(self.source)
        self.assertEqual(pin["bundle_sha256"], "9cc2a774b5198dcff14d9be1e66091f538975d867ce029a96bce15a55dfd730f")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cadata=bundle.decode())
        self.assertEqual(len(context.get_ca_certs()), pin["certificate_count"])

    def test_tamper_missing_symlink_and_extra_pin_field_rejected(self):
        bundle = self.source / "cacert.pem"
        original = bundle.read_bytes()
        bundle.write_bytes(original + b"tampered")
        with self.assertRaises(ValueError):
            CA.verify(self.source)
        bundle.unlink()
        with self.assertRaises(FileNotFoundError):
            CA.verify(self.source)
        bundle.symlink_to(LINUX / "ca-trust/cacert.pem")
        with self.assertRaises(ValueError):
            CA.verify(self.source)
        bundle.unlink()
        bundle.write_bytes(original)
        manifest = self.source / "inputs.json"
        manifest.write_text(json.dumps(json.loads(manifest.read_text()) | {"host_ca": "/etc/ssl"}))
        with self.assertRaises(ValueError):
            CA.verify(self.source)

    def test_install_exact_bytes_license_provenance_and_verified_empty_directory(self):
        CA.install(self.source, self.guest)
        self.assertEqual((self.guest / "etc/vz/ca-certificates.crt").read_bytes(), (self.source / "cacert.pem").read_bytes())
        self.assertEqual((self.guest / "usr/share/licenses/vz-ca-trust/LICENSE").read_bytes(), (self.source / "LICENSE").read_bytes())
        self.assertEqual((self.guest / "usr/share/licenses/vz-ca-trust/inputs.json").read_bytes(), (self.source / "inputs.json").read_bytes())
        self.assertEqual(list((self.guest / "etc/vz/empty-ca-directory").iterdir()), [])
        result = self.boot()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_assembly_foreign_destination_symlink_not_followed(self):
        foreign = self.root / "foreign"
        foreign.mkdir()
        (self.guest / "etc").symlink_to(foreign)
        with self.assertRaises(ValueError):
            CA.install(self.source, self.guest)
        self.assertEqual(list(foreign.iterdir()), [])

    def test_overlay_exact_copy_and_existing_same_bytes_made_readonly(self):
        CA.install(self.source, self.guest)
        overlay = self.root / "overlay"
        overlay.mkdir()
        result = self.boot(overlay)
        self.assertEqual(result.returncode, 0, result.stderr)
        ca = overlay / "etc/vz/ca-certificates.crt"
        self.assertEqual(ca.read_bytes(), (self.source / "cacert.pem").read_bytes())
        ca.chmod(0o644)
        result = self.boot(overlay)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(stat.S_IMODE(ca.stat().st_mode), 0o444)
        self.assertEqual(stat.S_IMODE((overlay / "etc/vz/empty-ca-directory").stat().st_mode), 0o555)

    def test_boot_rejects_modified_bundle_and_nonempty_alternate_directory(self):
        CA.install(self.source, self.guest)
        ca = self.guest / "etc/vz/ca-certificates.crt"
        original = ca.read_bytes()
        ca.chmod(0o644)
        ca.write_bytes(b"tampered")
        self.assertNotEqual(self.boot().returncode, 0)
        ca.write_bytes(original)
        empty = self.guest / "etc/vz/empty-ca-directory"
        empty.chmod(0o755)
        (empty / "extra.pem").write_bytes(original)
        self.assertNotEqual(self.boot().returncode, 0)

    def test_overlay_symlink_and_different_existing_ca_rejected(self):
        CA.install(self.source, self.guest)
        foreign = self.root / "foreign"
        foreign.mkdir()
        overlay = self.root / "overlay"
        overlay.mkdir()
        (overlay / "etc").symlink_to(foreign)
        self.assertNotEqual(self.boot(overlay).returncode, 0)
        self.assertEqual(list(foreign.iterdir()), [])
        (overlay / "etc").unlink()
        target = overlay / "etc/vz/ca-certificates.crt"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"foreign trust")
        self.assertNotEqual(self.boot(overlay).returncode, 0)
        self.assertEqual(target.read_bytes(), b"foreign trust")

    def test_distro_trust_regular_and_symlink_layouts_preserved(self):
        # A different, valid public CA fixture, without reading host trust.
        bundle = (self.source / "cacert.pem").read_bytes()
        distro_bytes = (b"-----BEGIN CERTIFICATE-----" +
                        bundle.split(b"-----BEGIN CERTIFICATE-----", 1)[1].split(
                            b"-----END CERTIFICATE-----", 1)[0] +
                        b"-----END CERTIFICATE-----\n")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cadata=distro_bytes.decode())
        self.assertEqual(len(context.get_ca_certs()), 1)
        self.assertNotEqual(distro_bytes, bundle)
        CA.install(self.source, self.guest)
        for phase in ("assembly", "overlay"):
            for layout in ("regular", "ssl-symlink", "bundle-symlink"):
                with self.subTest(phase=phase, layout=layout):
                    root = self.root / (phase + "-" + layout)
                    root.mkdir()
                    (root / "etc").mkdir()
                    ssl_path = root / "etc/ssl"
                    if layout == "ssl-symlink":
                        ssl_target = root / "distro-ssl"
                        ssl_target.mkdir()
                        ssl_path.symlink_to("../distro-ssl")
                    else:
                        ssl_path.mkdir()
                    (ssl_path / "certs").mkdir()
                    ca = ssl_path / "certs/ca-certificates.crt"
                    if layout == "bundle-symlink":
                        target = ssl_path / "distro-bundle.pem"
                        target.write_bytes(distro_bytes)
                        ca.symlink_to("../distro-bundle.pem")
                    else:
                        ca.write_bytes(distro_bytes)
                    paths = (ssl_path, ssl_path / "certs", ca, ca.resolve())
                    before = [(path.lstat(), os.readlink(path) if path.is_symlink() else None)
                              for path in paths]
                    if phase == "assembly":
                        CA.install(self.source, root)
                    else:
                        result = self.boot(root)
                        self.assertEqual(result.returncode, 0, result.stderr)
                    # A second boot preserves existing exact control-plane CA,
                    # and still must never modify the unrelated distro store.
                    result = self.boot(root)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    for path, (original, link) in zip(paths, before):
                        actual = path.lstat()
                        for field in ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size",
                                      "st_mtime_ns", "st_ctime_ns"):
                            self.assertEqual(getattr(actual, field), getattr(original, field))
                        if link is not None:
                            self.assertEqual(os.readlink(path), link)
                    self.assertEqual(ca.read_bytes(), distro_bytes)
                    self.assertEqual((root / "etc/vz/ca-certificates.crt").read_bytes(), bundle)

    def test_overlay_reserved_namespace_symlinks_and_hardlinks_rejected(self):
        CA.install(self.source, self.guest)
        for kind in ("namespace-symlink", "bundle-symlink", "bundle-hardlink"):
            with self.subTest(kind=kind):
                root = self.root / kind
                root.mkdir()
                (root / "etc").mkdir()
                foreign = self.root / (kind + "-foreign")
                foreign.mkdir()
                original = foreign / "ca-certificates.crt"
                original.write_bytes((self.source / "cacert.pem").read_bytes())
                original_stat = original.stat()
                reserved = root / "etc/vz"
                if kind == "namespace-symlink":
                    reserved.symlink_to(foreign)
                else:
                    reserved.mkdir()
                    if kind == "bundle-symlink":
                        (reserved / "ca-certificates.crt").symlink_to(original)
                    else:
                        os.link(original, reserved / "ca-certificates.crt")
                self.assertNotEqual(self.boot(root).returncode, 0)
                self.assertEqual(original.read_bytes(), (self.source / "cacert.pem").read_bytes())
                self.assertEqual(original.stat().st_ino, original_stat.st_ino)
                self.assertEqual(original.stat().st_mode, original_stat.st_mode)

    def test_make_inputs_are_developer_only_and_boot_failure_has_no_fallback(self):
        make = (LINUX / "Makefile").read_text()
        profile = make.split("ifeq ($(KERNEL_PROFILE),developer)\nINITRAMFS_PROFILE_INPUTS :=", 1)[1].split("endif", 1)[0]
        for filename in ("inputs.json", "cacert.pem", "LICENSE", "install.sh"):
            self.assertIn("$(CA_TRUST_SOURCE)/" + filename, profile.split("else", 1)[0])
        assembly = make.split("ifeq ($(KERNEL_PROFILE),developer)\n\tpython3", 1)[1].split("endif", 1)[0]
        self.assertIn('"$(CA_TRUST_HELPER)" --source "$(CA_TRUST_SOURCE)" --install-root "$(INITRAMFS_ROOT)"', assembly)
        init = (LINUX / "initramfs/init").read_text()
        for marker in ("Developer CA trust integrity failed", "Developer overlay CA trust installation failed"):
            self.assertIn("exit 1", init.split(marker, 1)[1].split("fi", 1)[0])
        self.assertIn("export SSL_CERT_FILE=/etc/vz/ca-certificates.crt", init)
        self.assertIn("export SSL_CERT_DIR=/etc/vz/empty-ca-directory", init)
        self.assertNotIn("etc/ssl", (self.source / "install.sh").read_text().replace(
            "# The control-plane namespace must not adopt or traverse distro /etc/ssl.", ""))
        self.assertLess(init.index('install_developer_ca_trust "$ROOTFS"'), init.index("starting guest agent on vsock"))
        for script in (LINUX / "initramfs/init", self.source / "install.sh"):
            result = subprocess.run(["/bin/sh", "-n", str(script)], capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
