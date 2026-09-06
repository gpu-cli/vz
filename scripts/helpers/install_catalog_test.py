"""Offline installer plumbing checks. No downloads, Docker, or VM execution."""

import os
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


class InstalledCatalogTests(unittest.TestCase):
    @contextmanager
    def alias_fixture(self):
        script = Path(__file__).resolve().parents[1] / "install.sh"
        source = script.read_text().removesuffix('main "$@"\n')
        with tempfile.TemporaryDirectory(prefix="vz-alias-test-") as temporary:
            root = Path(temporary).resolve()
            bundle = root / "source bundle"
            alias = root / "prefix with spaces/linux"
            bundle.mkdir()
            for name in ("vmlinux", "initramfs.img", "youki"):
                (bundle / name).write_bytes(name.encode())
            archive = bundle / "developer-probe-rootfs.tar"
            archive.write_bytes(b"digest-bound offline fixture")
            metadata = {"profile": "developer", "developer_probe": {
                "schema_version": 1, "archive": archive.name,
                "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}}
            (bundle / "version.json").write_text(json.dumps(metadata))

            def run():
                return subprocess.run(
                    ["/bin/bash", "-c", source + '\ncopy_linux_profile_to_legacy_default "$VZ_TEST_BUNDLE"\n'],
                    env=os.environ | {"VZ_INSTALL_DIR": str(alias.parent), "VZ_TEST_BUNDLE": str(bundle)},
                    capture_output=True, text=True, timeout=10)

            yield bundle, alias, metadata, run

    def test_alias_preserves_declared_probe_bytes_and_metadata_digest(self):
        with self.alias_fixture() as (bundle, alias, metadata, run):
            result = run()
            self.assertEqual(result.returncode, 0, result.stderr)
            copied = alias / metadata["developer_probe"]["archive"]
            self.assertEqual(copied.read_bytes(), (bundle / copied.name).read_bytes())
            self.assertEqual(hashlib.sha256(copied.read_bytes()).hexdigest(),
                             json.loads((alias / "version.json").read_text())["developer_probe"]["sha256"])

    def test_old_declared_probe_is_removed_only_when_new_metadata_has_none(self):
        for absent in (True, False):
            with self.subTest(absent=absent), self.alias_fixture() as (bundle, alias, metadata, run):
                self.assertEqual(run().returncode, 0)
                (bundle / "developer-probe-rootfs.tar").unlink()
                if absent:
                    metadata.pop("developer_probe")
                else:
                    metadata["developer_probe"] = None
                (bundle / "version.json").write_text(json.dumps(metadata))
                result = run()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse((alias / "developer-probe-rootfs.tar").exists())

    def test_changed_declared_probe_can_replace_its_exact_prior_artifact(self):
        with self.alias_fixture() as (bundle, alias, metadata, run):
            self.assertEqual(run().returncode, 0)
            archive = bundle / "developer-probe-rootfs.tar"
            archive.write_bytes(b"new verified archive")
            metadata["developer_probe"]["sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
            (bundle / "version.json").write_text(json.dumps(metadata))
            result = run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((alias / archive.name).read_bytes(), archive.read_bytes())

    def test_legacy_no_probe_stays_supported_without_hardened_adoption(self):
        for profile in (None, "developer", "container"):
            with self.subTest(profile=profile), self.alias_fixture() as (bundle, alias, metadata, run):
                (bundle / "developer-probe-rootfs.tar").unlink()
                metadata = {} if profile is None else {"profile": profile}
                (bundle / "version.json").write_text(json.dumps(metadata))
                result = run()
                self.assertEqual(result.returncode == 0, profile != "container", result.stderr)
                self.assertFalse((alias / "developer-probe-rootfs.tar").exists())

    def test_unknown_or_changed_previous_probe_is_preserved(self):
        for prior in ("undeclared", "digest_mismatch", "symlink", "hardlink"):
            with self.subTest(prior=prior), self.alias_fixture() as (bundle, alias, metadata, run):
                self.assertEqual(run().returncode, 0)
                target = alias / "developer-probe-rootfs.tar"
                if prior == "undeclared":
                    (alias / "version.json").write_text('{"profile":"developer"}')
                elif prior == "digest_mismatch":
                    target.write_bytes(b"foreign modified content")
                else:
                    foreign = alias.parent / "foreign"
                    target.rename(foreign)
                    if prior == "symlink":
                        target.symlink_to(foreign)
                    else:
                        os.link(foreign, target)
                before = target.read_bytes()
                metadata_before = (alias / "version.json").read_bytes()
                result = run()
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(target.read_bytes(), before)
                self.assertEqual((alias / "version.json").read_bytes(), metadata_before)

    def test_invalid_source_probe_fails_before_alias_changes(self):
        for invalid in ("missing", "tampered", "symlink", "hardlink", "redirected", "schema", "type", "hardened", "json", "root_null", "undeclared"):
            with self.subTest(invalid=invalid), self.alias_fixture() as (bundle, alias, metadata, run):
                archive = bundle / "developer-probe-rootfs.tar"
                if invalid == "missing":
                    archive.unlink()
                elif invalid == "tampered":
                    archive.write_bytes(b"modified")
                elif invalid in ("symlink", "hardlink"):
                    foreign = bundle.parent / "foreign"
                    archive.rename(foreign)
                    if invalid == "symlink":
                        archive.symlink_to(foreign)
                    else:
                        os.link(foreign, archive)
                elif invalid == "redirected":
                    metadata["developer_probe"]["archive"] = "../foreign"
                elif invalid == "schema":
                    metadata["developer_probe"]["schema_version"] = 2
                elif invalid == "type":
                    metadata["developer_probe"] = False
                elif invalid == "hardened":
                    metadata["profile"] = "container"
                elif invalid == "undeclared":
                    metadata.pop("developer_probe")
                value = "broken" if invalid == "json" else "null" if invalid == "root_null" else json.dumps(metadata)
                (bundle / "version.json").write_text(value)
                self.assertNotEqual(run().returncode, 0)
                self.assertFalse(alias.exists())

    def invoke(self, profile="all", missing_optional=False, no_linux=False):
        script = Path(__file__).resolve().parents[1] / "install.sh"
        source = script.read_text()
        self.assertTrue(source.endswith('main "$@"\n'))
        source = source.removesuffix('main "$@"\n')
        with tempfile.TemporaryDirectory(prefix="vz-install-test-") as temporary:
            root = Path(temporary).resolve()
            prefix = root / "prefix with spaces"
            (prefix / "bin").mkdir(parents=True)
            daemon = prefix / "bin/vz-runtimed"
            daemon.write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$VZ_TEST_OUTPUT"\n')
            daemon.chmod(0o700)
            output = root / "arguments"
            env = os.environ | {
                "VZ_INSTALL_DIR": str(prefix), "VZ_LINUX_PROFILE": profile,
                "VZ_NO_LINUX": "1" if no_linux else "0",
                "VZ_TEST_OUTPUT": str(output),
                "VZ_TEST_MISSING_OPTIONAL": "1" if missing_optional else "0",
            }
            # Replace download/extraction only. Execute the actual profile routing
            # and offline verifier invocation against a recording executable.
            stub = '''
install_linux_profile_artifacts() {
    if [ "$VZ_TEST_MISSING_OPTIONAL" = 1 ] && [ "${4:-required}" = optional ]; then
        return
    fi
    INSTALLED_LINUX_PROFILES+=("$2")
}
copy_linux_profile_to_legacy_default() { :; }
install_linux_artifacts 0.4.0-test
install_machine_catalog 0.4.0-test
'''
            result = subprocess.run(["/bin/bash", "-c", source + stub], env=env,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            args = output.read_text().splitlines() if output.exists() else []
            if args:
                self.assertEqual(args[:4], ["--write-installed-machine-target-catalog", str(prefix),
                                           "--installed-release-version", "0.4.0-test"])
            return args[4:]

    def test_both_profiles_are_explicit_and_prefix_with_spaces_is_one_argument(self):
        self.assertEqual(self.invoke(), ["--installed-linux-profile", "developer",
                                         "--installed-linux-profile", "container"])

    def test_missing_optional_profile_is_not_adopted_from_disk(self):
        self.assertEqual(self.invoke(missing_optional=True), ["--installed-linux-profile", "developer"])

    def test_single_selected_profiles_remain_qualified(self):
        for profile in ("developer", "container"):
            with self.subTest(profile=profile):
                self.assertEqual(self.invoke(profile), ["--installed-linux-profile", profile])

    def test_no_linux_does_not_generate_a_catalog_from_old_artifacts(self):
        self.assertEqual(self.invoke(no_linux=True), [])

    def test_installer_does_not_kill_live_machine_owner(self):
        source = (Path(__file__).resolve().parents[1] / "install.sh").read_text()
        self.assertNotIn("pkill", source)


if __name__ == "__main__":
    unittest.main()
