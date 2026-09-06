"""Offline structural/content tests; not runtime acceptance."""
import importlib.util
import errno
import io
import json
import os
from pathlib import Path
import shlex
import socket
import signal
import stat
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

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

    def test_public_output_mode_and_bytes_are_independent_of_inherited_umask(self):
        tools = module("fixture_output_mode", ROOT / "build/tools.py")
        payload = b"public-fixture-output\x00\n"
        for mask in (0o000, 0o022, 0o077):
            with self.subTest(mask=oct(mask)), tempfile.TemporaryDirectory(prefix="vz04-output-mode-") as directory:
                destination_root = Path(directory) / "out"

                def container_path(*parts):
                    self.assertEqual(parts[0], "/out")
                    return destination_root.joinpath(*parts[1:])

                previous = os.umask(mask)
                try:
                    with patch.object(tools, "Path", side_effect=container_path):
                        for name in ("payload.txt", "secret.txt", "cache.txt", "ssh.txt"):
                            tools.output(name, payload)
                            selected = destination_root / name
                            self.assertEqual(selected.read_bytes(), payload)
                            self.assertTrue(selected.is_file())
                            self.assertEqual(stat.S_IMODE(selected.stat().st_mode), 0o644)
                    self.assertEqual(os.umask(mask), mask, "output must not change process umask")
                finally:
                    os.umask(previous)

    def test_public_output_normalizes_existing_file_mode(self):
        tools = module("fixture_existing_output_mode", ROOT / "build/tools.py")
        with tempfile.TemporaryDirectory(prefix="vz04-existing-output-") as directory:
            destination_root = Path(directory) / "out"
            destination_root.mkdir()
            selected = destination_root / "payload.txt"
            for mode in (0o600, 0o666, 0o755):
                with self.subTest(mode=oct(mode)):
                    selected.write_bytes(b"previous payload")
                    selected.chmod(mode)

                    def container_path(*parts):
                        self.assertEqual(parts[0], "/out")
                        return destination_root.joinpath(*parts[1:])

                    previous = os.umask(0o077)
                    try:
                        with patch.object(tools, "Path", side_effect=container_path):
                            tools.output("payload.txt", b"new payload\n")
                        self.assertEqual(selected.read_bytes(), b"new payload\n")
                        self.assertEqual(stat.S_IMODE(selected.stat().st_mode), 0o644)
                        self.assertEqual(os.umask(0o077), 0o077)
                    finally:
                        os.umask(previous)

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

    def test_transport_http_errors_and_redirects_are_reachability_not_denial(self):
        service = module("fixture_transport_http", ROOT / "compose/service.py")
        url = "http://db:8080/health"
        for code in (200, 302, 401, 403, 404, 503):
            opener = Mock()
            if code == 200:
                response = Mock(status=code)
                opener.open.return_value.__enter__ = Mock(return_value=response)
                opener.open.return_value.__exit__ = Mock(return_value=False)
            else:
                opener.open.side_effect = HTTPError(url, code, "HTTP response", {}, io.BytesIO())
            with self.subTest(code=code), patch.object(service, "build_opener", return_value=opener) as build:
                value = service.transport(url)
                self.assertEqual(value, {"schema_version": 1, "url": url, "outcome": "http_response",
                                         "status": code, "errno": None, "exception": None})
                opener.open.assert_called_once_with(url, timeout=2)
                self.assertIsInstance(build.call_args.args[0], service.ProxyHandler)
                self.assertEqual(build.call_args.args[0].proxies, {})
                handler = build.call_args.args[1]
                self.assertIsInstance(handler, service.NoTransportRedirects)
                self.assertIsNone(handler.redirect_request(None, None, 302, "redirect", {}, "http://foreign/"))
                if code == 200:
                    response.read.assert_not_called()

    def test_transport_only_typed_network_errors_are_classified(self):
        service = module("fixture_transport_errors", ROOT / "compose/service.py")
        url = "http://172.18.0.2:8080/health"
        cases = [
            (socket.gaierror(socket.EAI_NONAME, "DNS absent"), "dns_failure"),
            (ConnectionRefusedError(errno.ECONNREFUSED, "refused"), "connection_refused"),
            (TimeoutError("deadline"), "timeout"),
            (OSError(errno.ETIMEDOUT, "deadline"), "timeout"),
            (OSError(errno.ENETUNREACH, "unreachable"), "network_unreachable"),
            (OSError(errno.EHOSTUNREACH, "unreachable"), "network_unreachable"),
            (PermissionError(errno.EACCES, "not network proof"), "probe_error"),
            (FileNotFoundError(errno.ENOENT, "not network proof"), "probe_error"),
            (ValueError("application bug"), "probe_error"),
            ("timed out", "probe_error"),
        ]
        for error, outcome in cases:
            opener = Mock()
            opener.open.side_effect = URLError(error)
            with self.subTest(error=error), patch.object(service, "build_opener", return_value=opener):
                value = service.transport(url)
                self.assertEqual(value["outcome"], outcome)
                self.assertEqual(value["url"], url)
                self.assertIsNone(value["status"])
                self.assertEqual(value["errno"], getattr(error, "errno", None))
                self.assertEqual(value["exception"], type(error).__name__)

    def test_transport_late_close_error_cannot_erase_observed_http_response(self):
        service = module("fixture_transport_late_error", ROOT / "compose/service.py")
        opener = Mock()
        opener.open.return_value.__enter__ = Mock(return_value=Mock(status=503))
        opener.open.return_value.__exit__ = Mock(side_effect=OSError(errno.ENETUNREACH, "late close"))
        with patch.object(service, "build_opener", return_value=opener):
            value = service.transport("http://db:8080/health")
        self.assertEqual(value["outcome"], "http_response")
        self.assertEqual(value["status"], 503)
        self.assertIsNone(value["errno"])
        self.assertIsNone(value["exception"])

    def test_transport_rejects_unscoped_urls_before_any_attempt(self):
        service = module("fixture_transport_urls", ROOT / "compose/service.py")
        for url in ("file:///etc/passwd", "https://db:8080/health", "http://db/health",
                    "http://db:8080/value", "http://user:secret@db:8080/health",
                    "http://db:8080/health?query", "http://db:8080/health#fragment",
                    "http://DB:8080/health", "http://db:8080/health\n", "x" * 321):
            with self.subTest(url=url), patch.object(service, "build_opener") as opener:
                self.assertEqual(service.transport(url)["outcome"], "probe_error")
                opener.assert_not_called()

    def test_transport_output_and_existing_positive_probe_are_distinct(self):
        service = module("fixture_transport_output", ROOT / "compose/service.py")
        url = "http://db:8080/health"
        for outcome, code in (("http_response", 0), ("dns_failure", 0), ("probe_error", 2)):
            value = {"schema_version": 1, "url": url, "outcome": outcome,
                     "status": 200 if outcome == "http_response" else None,
                     "errno": None, "exception": None}
            output = io.StringIO()
            with patch.object(service, "transport", return_value=value), patch.object(service.sys, "stdout", output):
                self.assertEqual(service.main(["transport", url]), code)
            self.assertEqual(output.getvalue(), json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        output = Mock(buffer=io.BytesIO())
        with patch.object(service, "fetch", return_value=b"exact payload\x00\n"), patch.object(service.sys, "stdout", output):
            self.assertEqual(service.main(["probe", url]), 0)
        self.assertEqual(output.buffer.getvalue(), b"exact payload\x00\n")

    def test_transport_total_deadline_bounds_even_a_blocked_resolver(self):
        service = module("fixture_transport_deadline", ROOT / "compose/service.py")
        previous = signal.getsignal(signal.SIGALRM)
        opener = Mock()
        opener.open.side_effect = lambda *_args, **_kwargs: time.sleep(20)
        started = time.monotonic()
        with patch.object(service, "build_opener", return_value=opener):
            value = service.transport("http://db:8080/health")
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(value["outcome"], "timeout")
        self.assertEqual(value["exception"], "TimeoutError")
        self.assertEqual(signal.getsignal(signal.SIGALRM), previous)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

    def test_transport_foreign_alarm_is_not_overwritten_or_network_denial(self):
        service = module("fixture_transport_foreign_alarm", ROOT / "compose/service.py")
        with patch.object(service.signal, "getitimer", return_value=(1.0, 0.0)), \
                patch.object(service.signal, "signal") as handler, \
                patch.object(service.signal, "setitimer") as timer, \
                patch.object(service, "build_opener") as opener:
            value = service.transport("http://db:8080/health")
        self.assertEqual(value["outcome"], "probe_error")
        opener.return_value.open.assert_not_called()
        handler.assert_not_called()
        timer.assert_not_called()

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
