"""Offline structural/content tests; not runtime acceptance."""
import importlib.util
import json
import os
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch

from validate import ROOT, validate, validate_inputs


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


class FixtureTests(unittest.TestCase):
    def test_subset_lint_does_not_certify_runtime(self):
        result = validate()
        self.assertEqual(result["docker_tests_executed"], 0)
        self.assertIs(result["compatibility_certified"], False)
        self.assertNotEqual(result["expected_payload_sha256"]["build_alpha"],
                            result["expected_payload_sha256"]["build_beta"])

    def test_mutable_missing_and_foreign_input_forms_fail(self):
        # Synthetic syntax specimens only, never claimed to name available artifacts.
        base, image = "fixture.invalid/base@sha256:" + "a" * 64, "sha256:" + "b" * 64
        validate_inputs(base, image, "fixture-owner")
        for changed in [("", image, "owner"), ("python:latest", image, "owner"),
                        ("python@sha256:bad", image, "owner"), (base, "fixture:local", "owner"),
                        (base, image, ""), (base, image, "../foreign"), (base, image, "a\nb")]:
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    validate_inputs(*changed)

    def test_embedded_dockerfile_base_guards_reject_mutable_references(self):
        for relative, variable in [("compose/Dockerfile", "FIXTURE_BASE"),
                                   ("ssh/Dockerfile", "FIXTURE_SSH_BASE")]:
            line = next(line for line in (ROOT / relative).read_text().splitlines()
                        if line.startswith("RUN python3 -c"))
            code = shlex.split(line)[3]
            with patch.dict(os.environ, {variable: "fixture.invalid/base@sha256:" + "a" * 64}):
                exec(compile(code, relative, "exec"), {})
            with patch.dict(os.environ, {variable: "fixture:latest"}):
                with self.assertRaises(AssertionError):
                    exec(compile(code, relative, "exec"), {})

    def test_payload_and_secret_workload_matches_manifest_without_container_paths(self):
        tools = module("fixture_build_tools", ROOT / "build/tools.py")
        expected = json.loads((ROOT / "fixture.json").read_text())["expected"]
        with patch.dict(os.environ, {"FIXTURE_BASE": "fixture.invalid/base@sha256:" + "a" * 64,
                                    "FIXTURE_VARIANT": "alpha", "FIXTURE_RUN": "candidate"}):
            with patch.object(tools.Path, "read_bytes", return_value=b"vz04-build-input-v1\n"):
                with patch.object(tools, "output") as output:
                    tools.main("payload")
                    output.assert_called_once_with("payload.txt", expected["build_alpha"].encode())
        # Wrong secrets fail before producing any artifact; no secret bytes printed.
        with patch.dict(os.environ, {"FIXTURE_BASE": "fixture.invalid/base@sha256:" + "a" * 64,
                                    "FIXTURE_SECRET_SHA256": "0" * 64}):
            with patch.object(tools.Path, "read_bytes", return_value=b"wrong secret"):
                with patch.object(tools, "output") as output:
                    with self.assertRaises(ValueError):
                        tools.main("secret")
                    output.assert_not_called()

    def test_compose_owner_records_and_exit_payloads_are_exact(self):
        service = module("fixture_service", ROOT / "compose/service.py")
        expected = json.loads((ROOT / "fixture.json").read_text())["expected"]
        with patch.dict(os.environ, {"FIXTURE_OWNER": "machine-a"}):
            self.assertEqual(service.record("db", "persisted").decode(),
                             expected["persistence_template"].format(owner="machine-a"))
            self.assertEqual(service.record("api", "exec-stdout").decode(),
                             expected["exec_stdout_template"].format(owner="machine-a"))
        with patch.dict(os.environ, {"FIXTURE_OWNER": "../foreign"}):
            with self.assertRaises(ValueError):
                service.owner()

    def test_cache_mount_rejects_cold_reuse_and_foreign_warm_owner(self):
        tools = module("fixture_cache_tools", ROOT / "build/tools.py")
        with tempfile.TemporaryDirectory(prefix="vz04-cache-offline-") as directory:
            marker = Path(directory) / "owner"
            with patch.dict(os.environ, {"FIXTURE_BASE": "fixture.invalid/base@sha256:" + "a" * 64,
                                        "FIXTURE_OWNER": "machine-a", "FIXTURE_CACHE_EXPECT": "cold",
                                        "FIXTURE_CACHE_STEP": "first"}):
                with patch.object(tools, "Path", return_value=marker), patch.object(tools, "output"):
                    tools.main("cache")
                    self.assertEqual(marker.read_bytes(), b"machine-a\n")
                    with self.assertRaises(ValueError):
                        tools.main("cache")
                    os.environ["FIXTURE_CACHE_EXPECT"] = "warm"
                    tools.main("cache")
                    os.environ["FIXTURE_OWNER"] = "machine-b"
                    with self.assertRaises(ValueError):
                        tools.main("cache")

    def test_ssh_requires_actual_success_and_exact_server_bytes(self):
        tools = module("fixture_ssh_tools", ROOT / "build/tools.py")
        import subprocess
        with patch.dict(os.environ, {"FIXTURE_BASE": "fixture.invalid/base@sha256:" + "a" * 64,
                                    "FIXTURE_SSH_HOST": "owned-ssh", "FIXTURE_SSH_PORT": "2222"}):
            for code, stdout in [(255, b""), (0, b"foreign server\n")]:
                with patch.object(tools.subprocess, "run", return_value=subprocess.CompletedProcess(
                        [], code, stdout=stdout, stderr=b"")), patch.object(tools.Path, "read_bytes",
                        return_value=b"vz04-ssh-authenticated-v1\n"), patch.object(tools, "output") as output:
                    with self.assertRaises(ValueError):
                        tools.main("ssh")
                    output.assert_not_called()

    def test_credential_material_is_outside_every_build_context(self):
        secret = (ROOT / "inputs/secret.txt").read_bytes()
        for directory in ("build", "compose", "ssh"):
            for path in (ROOT / directory).iterdir():
                if path.is_file():
                    self.assertNotIn(secret, path.read_bytes(), str(path))
        ssh = (ROOT / "ssh/sshd_config").read_text()
        for directive in ("PasswordAuthentication no", "AllowTcpForwarding no",
                          "AllowAgentForwarding no", "ForceCommand /bin/cat /fixture/response.txt"):
            self.assertIn(directive, ssh)

    def test_network_health_and_scope_mutations_fail_lint(self):
        # Change only JSON inputs in a private copy; do not start workloads.
        for mutation in ("health", "network", "privilege", "claim"):
            with tempfile.TemporaryDirectory(prefix="vz04-fixture-offline-") as directory:
                import shutil
                root = Path(directory) / "fixture"
                shutil.copytree(ROOT, root, ignore=shutil.ignore_patterns("__pycache__"))
                path = root / ("fixture.json" if mutation == "claim" else "compose/compose.json")
                value = json.loads(path.read_text())
                if mutation == "health":
                    value["services"]["api"]["depends_on"]["db"]["condition"] = "service_started"
                elif mutation == "network":
                    value["services"]["worker"]["networks"].append("backend")
                elif mutation == "privilege":
                    value["services"]["db"]["privileged"] = True
                else:
                    value["compatibility_certified"] = True
                path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    validate(root)


if __name__ == "__main__":
    unittest.main()
