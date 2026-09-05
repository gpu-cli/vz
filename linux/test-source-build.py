#!/usr/bin/env python3
"""Offline contract tests; extraction success here is not Linux build evidence."""

import argparse
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("source_build", Path(__file__).with_name("source-build.py"))
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class SourceBuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vz-source-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        compiler = mock.patch.object(BUILD, "compiler_identity", return_value="fixture compiler")
        compiler.start()
        self.addCleanup(compiler.stop)

    def wrapper_preflight(self, profile, target, cache_override):
        environment = {"PATH": "", "LINUX_DOCKER_TARGET": target}
        if cache_override is not None:
            environment["YOUKI_CACHE_DIR"] = cache_override
        return subprocess.run([
            "/bin/bash", str(Path(__file__).with_name("docker-build.sh")),
            str(self.root), str(self.root / "output"), str(self.root / "cache"), profile, "2",
        ], env=environment, capture_output=True, text=True, timeout=2, check=False)

    def test_full_wrapper_rejects_youki_cache_override_before_docker_or_output_writes(self):
        for profile in ("developer", "container"):
            with self.subTest(profile=profile):
                result = self.wrapper_preflight(profile, "all", "/external/youki-cache")
                self.assertEqual(result.returncode, 2)
                self.assertIn("unset YOUKI_CACHE_DIR", result.stderr)
                self.assertIn("external cache overrides are not mounted", result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertFalse((self.root / "output").exists())
                self.assertFalse((self.root / "cache").exists())

    def test_wrapper_cache_guard_allows_default_and_non_youki_targets(self):
        for target, override in (("all", None), ("all", ""), ("kernel", "/external/cache"), ("source-check", "/external/cache")):
            with self.subTest(target=target, override=override):
                result = self.wrapper_preflight("developer", target, override)
                self.assertEqual(result.returncode, 2)
                self.assertIn("set LINUX_DOCKER_CONTEXT explicitly", result.stderr)
                self.assertNotIn("unset YOUKI_CACHE_DIR", result.stderr)

    def archive(self, entries=None):
        archive = self.root / "source.tar"
        with tarfile.open(archive, "w") as stream:
            directory = tarfile.TarInfo("pkg")
            directory.type, directory.mode, directory.mtime = tarfile.DIRTYPE, 0o775, 1000
            stream.addfile(directory)
            for name, content, kind in entries or [("pkg/input", b"pinned source", "file")]:
                item = tarfile.TarInfo(name)
                item.mode, item.mtime = 0o664, 1000
                if kind == "symlink":
                    item.type, item.linkname = tarfile.SYMTYPE, content.decode()
                    stream.addfile(item)
                else:
                    item.size = len(content)
                    stream.addfile(item, io.BytesIO(content))
        return archive, BUILD.digest(archive)

    def prepare(self, archive, checksum):
        # The Mac may intentionally lack case-sensitive storage. These small
        # fixtures have no case collisions; only the storage probe is mocked.
        with mock.patch.object(BUILD, "require_case_sensitive"):
            return BUILD.prepare_source(archive, checksum, "pkg", self.root / "source", self.root / "build")

    def test_inventory_binds_every_case_distinct_path_and_content(self):
        archive, checksum = self.archive([
            ("pkg/xt_TCPMSS.h", b"upper", "file"),
            ("pkg/xt_tcpmss.h", b"lower", "file"),
        ])
        entries = BUILD.archive_inventory(archive, checksum, "pkg")
        self.assertEqual(len(entries), 3)
        self.assertNotEqual(entries[1]["path"], entries[2]["path"])
        self.assertNotEqual(entries[1]["sha256"], entries[2]["sha256"])

    def test_archive_digest_mismatch_is_rejected(self):
        archive, _ = self.archive()
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            BUILD.archive_inventory(archive, "0" * 64, "pkg")

    def test_build_environment_discards_compiler_make_and_config_overrides(self):
        overrides = {name: "untrusted override" for name in (
            "CC", "HOSTCC", "LD", "AR", "KCFLAGS", "KCPPFLAGS", "CFLAGS", "CPPFLAGS",
            "LDFLAGS", "MAKEFLAGS", "MAKEOVERRIDES", "KCONFIG_CONFIG", "KBUILD_OUTPUT",
            "KBUILD_BUILD_TIMESTAMP", "KBUILD_BUILD_USER", "SOURCE_DATE_EPOCH",
        )}
        with mock.patch.dict(os.environ, overrides):
            environment = BUILD.build_environment(1000)
        for name in overrides:
            self.assertNotEqual(environment.get(name), overrides[name], name)
        self.assertEqual(environment["SOURCE_DATE_EPOCH"], "1000")
        self.assertEqual(environment["KBUILD_BUILD_USER"], "vz")
        self.assertEqual(environment["LC_ALL"], "C")
        self.assertEqual(environment["TZ"], "UTC")

    def test_duplicate_paths_are_rejected(self):
        archive, checksum = self.archive([
            ("pkg/input", b"first", "file"), ("pkg/input", b"second", "file"),
        ])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            BUILD.archive_inventory(archive, checksum, "pkg")

    def test_traversal_and_escaping_symlinks_are_rejected(self):
        for entries in [
            [("pkg/../outside", b"escape", "file")],
            [("pkg/link", b"../../outside", "symlink")],
            [("pkg/link", b"/outside", "symlink")],
            [("pkg/link", b"input", "symlink"), ("pkg/link/child", b"bad", "file")],
        ]:
            with self.subTest(entries=entries):
                archive, checksum = self.archive(entries)
                with self.assertRaises(ValueError):
                    BUILD.archive_inventory(archive, checksum, "pkg")

    def test_case_insensitive_storage_fails_before_extraction(self):
        archive, checksum = self.archive()
        with mock.patch.object(BUILD, "require_case_sensitive", side_effect=ValueError("case-insensitive")):
            with mock.patch.object(BUILD.subprocess, "run") as command:
                with self.assertRaisesRegex(ValueError, "case-insensitive"):
                    BUILD.prepare_source(archive, checksum, "pkg", self.root / "source", self.root / "build")
                command.assert_not_called()
        self.assertFalse((self.root / "source").exists())

    def test_existing_source_is_verified_without_replacement(self):
        archive, checksum = self.archive()
        original = self.prepare(archive, checksum)
        source = self.root / "source/input"
        before = source.stat()
        self.assertEqual(self.prepare(archive, checksum), original)
        after = source.stat()
        self.assertEqual((before.st_ino, before.st_mtime_ns), (after.st_ino, after.st_mtime_ns))
        self.assertEqual(source.read_bytes(), b"pinned source")
        self.assertEqual(original["entry_count"], 2)
        self.assertEqual(original["source_date_epoch"], 1000)

    def test_existing_source_drift_is_not_repaired_or_deleted(self):
        archive, checksum = self.archive()
        self.prepare(archive, checksum)
        source = self.root / "source/input"
        source.write_bytes(b"user changes")
        before = source.stat()
        with self.assertRaisesRegex(ValueError, "mismatch"):
            self.prepare(archive, checksum)
        self.assertEqual(source.read_bytes(), b"user changes")
        self.assertEqual(source.stat().st_ino, before.st_ino)

    def test_extra_source_file_is_rejected_and_preserved(self):
        archive, checksum = self.archive()
        self.prepare(archive, checksum)
        extra = self.root / "source/extra"
        extra.write_bytes(b"untracked source")
        with self.assertRaisesRegex(ValueError, "inventory"):
            self.prepare(archive, checksum)
        self.assertEqual(extra.read_bytes(), b"untracked source")

    def test_source_provenance_drift_is_not_overwritten(self):
        archive, checksum = self.archive()
        self.prepare(archive, checksum)
        evidence = self.root / "source.source.json"
        evidence.write_bytes(b"foreign evidence")
        with self.assertRaisesRegex(ValueError, "provenance differs"):
            self.prepare(archive, checksum)
        self.assertEqual(evidence.read_bytes(), b"foreign evidence")

    def artifact_args(self):
        archive, checksum = self.archive()
        self.prepare(archive, checksum)
        artifact, config, effective, recipe = [self.root / name for name in ("vmlinux", "fragment", ".config", "recipe")]
        for path, content in [(artifact, b"image"), (config, b"CONFIG_SAMPLE=y\n"), (effective, b"effective config"), (recipe, b"recipe")]:
            path.write_bytes(content)
            path.chmod(0o644)
        return argparse.Namespace(
            artifact=str(artifact), profile="developer", config=str(config), effective_config=str(effective),
            source_manifest=str(self.root / "source.source.json"), recipe=[str(recipe)],
            compiler="gcc", sha256=checksum, kind="kernel", arch="arm64", cross_compile="", base_config="defconfig",
        )

    def test_artifact_provenance_roundtrip_and_input_drift_refusal(self):
        args = self.artifact_args()
        with mock.patch.object(BUILD.subprocess, "check_output", return_value="fixture compiler"):
            BUILD.record_artifact(args)
        BUILD.verify_artifact(args)
        for field, value in [("profile", "container"), ("sha256", "0" * 64), ("arch", "x86"), ("base_config", "allnoconfig")]:
            changed = copy.deepcopy(args)
            setattr(changed, field, value)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "provenance"):
                BUILD.verify_artifact(changed)
        Path(args.recipe[0]).write_bytes(b"changed build recipe")
        with self.assertRaisesRegex(ValueError, "provenance"):
            BUILD.verify_artifact(args)

    def test_artifact_bytes_mode_and_builder_drift_are_rejected(self):
        args = self.artifact_args()
        with mock.patch.object(BUILD.subprocess, "check_output", return_value="fixture compiler"):
            BUILD.record_artifact(args)
        with mock.patch.dict(os.environ, {"VZ_LINUX_BUILDER_ID": "sha256:" + "0" * 64}):
            with self.assertRaisesRegex(ValueError, "provenance"):
                BUILD.verify_artifact(args)
        with mock.patch.object(BUILD, "compiler_identity", return_value="changed compiler"):
            with self.assertRaisesRegex(ValueError, "provenance"):
                BUILD.verify_artifact(args)
        Path(args.artifact).chmod(0o755)
        with self.assertRaisesRegex(ValueError, "provenance"):
            BUILD.verify_artifact(args)
        Path(args.artifact).chmod(0o644)
        Path(args.artifact).write_bytes(b"other")
        with self.assertRaisesRegex(ValueError, "provenance"):
            BUILD.verify_artifact(args)


    def test_digest_rejects_fifo_symlink_and_hardlink_without_reading(self):
        source = self.root / "source"
        source.write_bytes(b"preserved source")
        link = self.root / "link"
        link.symlink_to(source)
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        for path in (link, fifo):
            with self.subTest(path=path), self.assertRaises((OSError, ValueError)):
                BUILD.digest(path)
        hardlink = self.root / "hardlink"
        os.link(source, hardlink)
        for path in (source, hardlink):
            with self.subTest(path=path), self.assertRaises(ValueError):
                BUILD.digest(path)
        self.assertEqual(source.read_bytes(), b"preserved source")

    def test_late_provenance_failure_preserves_previous_artifact(self):
        args = self.artifact_args()
        args.source = str(self.root / "source")
        args.build_dir = str(self.root / "build")
        args.archive = str(self.root / "source.tar")
        args.jobs = 1
        build = Path(args.build_dir)
        build.mkdir()
        before = Path(args.artifact).read_bytes()

        def make(*unused_args, **unused_kwargs):
            (build / ".config").write_bytes(b"CONFIG_SAMPLE=y\n")
            image = build / "arch/arm64/boot/Image"
            image.parent.mkdir(parents=True, exist_ok=True)
            image.write_bytes(b"newly built image")

        with mock.patch.object(BUILD, "require_case_sensitive"), mock.patch.object(BUILD.subprocess, "run", side_effect=make):
            with mock.patch.object(BUILD, "compiler_identity", side_effect=OSError("compiler identity unavailable")):
                with self.assertRaisesRegex(OSError, "compiler identity unavailable"):
                    BUILD.build_artifact(args)
        self.assertEqual(Path(args.artifact).read_bytes(), before)
        self.assertFalse(Path(str(args.artifact) + ".build.json").exists())

    def test_publication_refuses_foreign_evidence_type_without_replacing_output(self):
        pending, output = self.root / "candidate", self.root / "published"
        pending.write_bytes(b"new image")
        Path(str(pending) + ".build.json").write_bytes(b"new provenance")
        output.write_bytes(b"old image")
        os.mkfifo(str(output) + ".build.json")
        with self.assertRaises(ValueError):
            BUILD.publish_artifact(pending, output)
        self.assertEqual(output.read_bytes(), b"old image")
        self.assertEqual(pending.read_bytes(), b"new image")

    def test_second_publication_failure_retains_previous_pair_for_recovery(self):
        pending, output = self.root / "candidate", self.root / "published"
        pending.write_bytes(b"new image")
        Path(str(pending) + ".build.json").write_bytes(b"new provenance")
        output.write_bytes(b"old image")
        Path(str(output) + ".build.json").write_bytes(b"old provenance")
        original_replace = os.replace

        def replace(source, target):
            if str(source) == str(pending) + ".build.json":
                raise OSError("injected provenance publication failure")
            return original_replace(source, target)

        with mock.patch.object(BUILD.os, "replace", side_effect=replace):
            with self.assertRaisesRegex(OSError, "publication failure"):
                BUILD.publish_artifact(pending, output)
        retained = list(self.root.glob(".previous-published-*"))
        self.assertEqual(len(retained), 1)
        self.assertEqual((retained[0] / "published").read_bytes(), b"old image")
        self.assertEqual((retained[0] / "published.build.json").read_bytes(), b"old provenance")


if __name__ == "__main__":
    unittest.main()
