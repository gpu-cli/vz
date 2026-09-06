"""Offline OpenSSH input admission and exact staging regression tests."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import docker_host_driver as driver
import linux_docker_ssh_input as inputs


class SSHInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vz-ssh-input-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def test_checked_in_pin_and_guest_projection(self):
        pin = inputs.load()
        self.assertEqual(len(pin["packages"]), 8)
        fixture = inputs.REPO / "tests/fixtures/vz-0.4/docker-ssh/package-pins.json"
        self.assertEqual(inputs.guest_manifest(pin), json.loads(fixture.read_bytes()))
        self.assertEqual(sum(row["size"] for row in pin["packages"]), 1763884)

    def test_modified_pin_is_not_admitted(self):
        path = self.root / "pin.json"
        raw = inputs.PIN.read_bytes()
        for changed in (raw + b" ", raw.replace(b"arm64", b"amd64"), b"{}\n"):
            path.write_bytes(changed)
            with self.assertRaises(ValueError):
                inputs.load(path)

    def test_pin_cannot_rebind_base(self):
        base = inputs.image_input.load(inputs.IMAGE)
        for key, value in (("reference", "another"), ("config_digest", "sha256:" + "a" * 64)):
            with patch.object(inputs.image_input, "load", return_value={**base, key: value}):
                with self.assertRaises(ValueError):
                    inputs.load()

    def test_symlink_and_noncanonical_paths_rejected(self):
        path = self.root / "input"
        path.write_bytes(b"data")
        link = self.root / "alias"
        link.symlink_to(path)
        for selected in (link, self.root / ".." / self.root.name / "input", Path("input")):
            with self.assertRaises(ValueError):
                inputs.canonical(selected)

    def row(self):
        raw = b"authenticated package placeholder"
        row = {"filename": "test.deb", "sha256": driver.sha256(raw), "size": len(raw),
               "package": "test", "architecture": "arm64", "version": "1.0"}
        (self.root / row["filename"]).write_bytes(raw)
        return raw, row

    def test_read_input_has_exact_identity(self):
        raw, row = self.row()
        self.assertEqual(inputs.read_input(self.root, row), raw)
        for changed in ({**row, "filename": "../test.deb"}, {**row, "size": True},
                        {**row, "sha256": "0" * 64}):
            with self.assertRaises(ValueError):
                inputs.read_input(self.root, changed)

    def test_hardlinked_input_rejected(self):
        _, row = self.row()
        os.link(self.root / row["filename"], self.root / "alias")
        with self.assertRaises(ValueError):
            inputs.read_input(self.root, row)

    def test_stages_only_exact_public_debs(self):
        raw, row = self.row()
        (self.root / "must-not-copy").write_bytes(b"unrelated")
        destination = self.root / "packages"
        pin = {"packages": [row], "base": {"dpkg_deb": {"sha256": "1" * 64}}}
        with patch.object(inputs, "load", return_value=pin):
            proof = inputs.stage_packages(self.root, destination)
        self.assertEqual({item.name for item in destination.iterdir()}, {"test.deb", "manifest.json"})
        self.assertEqual((destination / "test.deb").read_bytes(), raw)
        self.assertEqual((destination / "test.deb").stat().st_mode & 0o777, 0o600)
        self.assertEqual(proof["tree_sha256"], driver.tree_digest(destination))

    def test_staging_rejects_tampered_input_before_creating_output(self):
        _, row = self.row()
        pin = {"packages": [{**row, "sha256": "0" * 64}]}
        destination = self.root / "packages"
        with patch.object(inputs, "load", return_value=pin), self.assertRaises(ValueError):
            inputs.stage_packages(self.root, destination)
        self.assertFalse(destination.exists())

    def test_staging_never_overwrites_existing_directory(self):
        _, row = self.row()
        destination = self.root / "packages"
        destination.mkdir()
        (destination / "sentinel").write_bytes(b"preserve")
        with patch.object(inputs, "load", return_value={"packages": [row]}), self.assertRaises(ValueError):
            inputs.stage_packages(self.root, destination)
        self.assertEqual((destination / "sentinel").read_bytes(), b"preserve")


if __name__ == "__main__":
    unittest.main()
