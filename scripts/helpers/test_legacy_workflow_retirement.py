#!/usr/bin/env python3
"""Execute retired entry points; never invoke an installer, VM, or daemon."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = (
    "run-vz-linux-vm-e2e.sh",
    "run-vz-linux-vm-e2e-local.sh",
    "run-vz-linux-vm-e2e-hostboot.sh",
    "run-vz-linux-hostboot-command.sh",
    "run-linux-daemon-release-gate.sh",
)


class LegacyWorkflowRetirementTests(unittest.TestCase):
    def test_every_entry_point_rejects_before_effects(self):
        with tempfile.TemporaryDirectory(prefix="vz-retired-workflows-") as tmp:
            root = Path(tmp)
            dependencies = root / "bin"
            dependencies.mkdir()
            calls = root / "dependency-calls"
            trap = '#!/bin/bash\nprintf "called\\n" >> "$VZ_TEST_DEPENDENCY_CALLS"\nexit 99\n'
            for command in (
                "vz", "vz-runtimed", "vz-api", "cargo", "docker", "sudo", "ssh",
                "curl", "mkdir", "uname", "date", "dirname", "sleep", "mount",
                "ln", "rm", "kill", "pkill", "mktemp", "cat", "python3",
            ):
                target = dependencies / command
                target.write_text(trap)
                target.chmod(0o755)
            state = root / "state"
            state.mkdir()
            sentinel = state / "sentinel"
            sentinel.write_bytes(b"unrelated persistent data\x00\xff")
            (root / "vz.json").write_text("malformed definition must not be read")
            before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
            env = {
                "PATH": str(dependencies),
                "VZ_BIN": str(dependencies / "vz"),
                "VZ_TEST_DEPENDENCY_CALLS": str(calls),
                "VZ_RUNTIME_DATA_DIR": str(state),
                "VZ_TEST_BTRFS_WORKSPACE": str(state),
                "VZ_LINUX_VM_IMAGE": str(sentinel),
                "VZ_LINUX_VM_NAME": "existing-user-machine",
            }
            variants = (
                [], ["--help"], ["--unknown"],
                ["--workspace", str(state), "--profile", "release", "--auto-start",
                 "--vm-image", str(sentinel), "--output-dir", str(root / "output"),
                 "--mount", "workspace:" + str(state), "--force"],
            )
            for name in WORKFLOWS:
                for args in variants:
                    with self.subTest(workflow=name, args=args):
                        result = subprocess.run(
                            [str(ROOT / "scripts" / name), *args],
                            cwd=root, env=env, capture_output=True, text=True, timeout=5,
                        )
                        self.assertEqual(result.returncode, 2, result.stderr)
                        self.assertEqual(result.stdout, "")
                        self.assertEqual(len(result.stderr.splitlines()), 1)
                        error = json.loads(result.stderr)["error"]
                        self.assertEqual(error["code"], "legacy_workflow_removed")
                        self.assertEqual(error["workflow"], name)
                        self.assertIn("not an equivalent workflow", error["backend_verification"])
                        self.assertFalse(calls.exists(), "retired helper invoked a dependency")
                        self.assertFalse((root / "output").exists())
                        after = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
                        self.assertEqual(before, after)

    def test_installer_guidance_uses_exact_binary_without_running_install(self):
        script = (ROOT / "scripts/install.sh").read_text()
        footer = '\nmain "$@"\n'
        self.assertTrue(script.endswith(footer))
        # Source definitions only, deliberately omitting the sole entry-point call.
        definitions = script[:-len(footer)]
        with tempfile.TemporaryDirectory(prefix="vz-install-guidance-") as tmp:
            install_dir = Path(tmp) / "prefix with spaces'and quotes"
            env = dict(os.environ, VZ_INSTALL_DIR=str(install_dir))
            result = subprocess.run(
                ["/bin/bash", "-c", definitions + "\nprint_getting_started\n"],
                cwd=tmp, env=env, capture_output=True, text=True, timeout=5,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            command = result.stdout.splitlines()[1].strip()
            self.assertEqual(shlex.split(command), [str(install_dir / "bin/vz"), "--help"])
            self.assertNotIn("vz run", result.stdout)
            self.assertNotIn('"image":', result.stdout)
            self.assertIn("depend on the installed version", result.stdout)
            self.assertFalse(install_dir.exists())

    def test_installed_runner_help_and_invalid_arguments_do_not_start_builds(self):
        with tempfile.TemporaryDirectory(prefix="vz-installed-runner-help-") as tmp:
            for arguments, code in [(["--help"], 0), (["--suite", "all"], 2)]:
                result = subprocess.run(
                    ["/bin/bash", str(ROOT / "scripts/run-installed-topology-cli-tests.sh"), *arguments],
                    cwd=tmp, env={"PATH": tmp}, capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertIn("control-plane", result.stdout + result.stderr)
                self.assertEqual(list(Path(tmp).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
