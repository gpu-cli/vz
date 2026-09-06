import base64
import io
import json
from pathlib import Path
import tarfile
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import linux_docker_buildkit_keep as keep
import linux_docker_e2e as gate


TOKEN = "vzbuild-" + "a" * 24
CASE = "keep-normal"


def fields(kept):
    root, cid, group = keep.identity(TOKEN, CASE)
    result = {name: keep.ABSENT for name in keep.FIELDS}
    result.update(owner=(TOKEN + "\n").encode(), root_identity=b"12:34",
                  state_present=b"1" if kept else b"0", cgroup_present=b"1" if kept else b"0")
    if kept:
        result.update(state=json.dumps({"id": cid, "status": "stopped", "pid": None,
                                        "bundle": root + "/" + CASE}).encode(),
                      config=json.dumps({"cgroup_path": group}).encode(), fs=b"63677270", type=b"domain\n",
                      procs=b"", events=b"populated 0\nfrozen 0\n", cpu=b"50000 100000\n", pids=b"64\n")
    return result


def frame(values):
    return b"VZ_YOUKI_KEEP_V1\n" + b"".join(name.encode() + b"=" + base64.b64encode(values[name]) + b"\n"
                                           for name in keep.FIELDS) + b"VZ_YOUKI_KEEP_END\n"


class ConfigurationTests(unittest.TestCase):
    def test_keep_marker_is_a_commit_component_not_necessarily_last(self):
        prefix = b"youki version: 0.7.0\ncommit: 0.7.0-pinned"
        for suffix in (b"+vz-run-keep-v1", b"+vz-run-keep-v1+vz-foreground-wait-v1"):
            self.assertTrue(keep.has_keep_patch(prefix + suffix + b"\nspec: 1.1.0\n"))
        for raw in (prefix + b"+vz-run-keep-v10\n", prefix + b"+not-vz-run-keep-v1\n",
                    b"spec: +vz-run-keep-v1\n", prefix + b"\n+vz-run-keep-v1\n",
                    prefix + b"+vz-run-keep-v1\ncommit: different\n"):
            with self.subTest(raw=raw):
                self.assertFalse(keep.has_keep_patch(raw))

    def test_deterministic_scoped_bundle(self):
        data = keep.bundle(TOKEN)
        self.assertEqual(data, keep.bundle(TOKEN))
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            for member in archive.getmembers():
                self.assertFalse(member.name.startswith("/"))
                self.assertNotIn("..", member.name.split("/"))
                self.assertFalse(member.issym() or member.islnk())
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, 0))
            for case, _, code, _ in keep.CASES:
                config = json.loads(archive.extractfile(case + "/config.json").read())
                self.assertEqual(config, keep.configuration(TOKEN, case, code))
                self.assertEqual(config["linux"]["cgroupsPath"], "/" + TOKEN + "-" + case)
                self.assertEqual(config["process"]["args"][:3], ["/bin/busybox", "sh", "-c"])
                self.assertTrue(config["root"]["readonly"])
                self.assertIn({"type": "network"}, config["linux"]["namespaces"])
                self.assertEqual(next(m for m in config["mounts"] if m["destination"] == "/dev")["type"], "tmpfs")

    def test_reject_unowned_identifiers(self):
        for token, case in (("../foreign", CASE), (TOKEN, "../other"), (TOKEN + "x", CASE)):
            with self.subTest(token=token, case=case), self.assertRaises(ValueError):
                keep.identity(token, case)

    def test_expected_nonzero_exit_is_explicitly_acknowledged(self):
        for case, _, code, _ in keep.CASES:
            marker = ("vz-keep-payload:" + TOKEN + "-" + case + "\nVZ_YOUKI_KEEP_EXIT=" + str(code) + "\n").encode()
            keep.run_ack(marker, b"", TOKEN, case, code)
            for raw, error in ((b"", b""), (marker + marker, b""), (marker, b"runtime error"),
                               (marker.replace(b"EXIT=37", b"EXIT=1") if code == 37 else marker[:-1], b"")):
                with self.subTest(case=case, raw=raw), self.assertRaises(ValueError):
                    keep.run_ack(raw, error, TOKEN, case, code)
        self.assertIn('test "$code" -eq "$expected"', keep.RUN_SCRIPT)


class ObservationTests(unittest.TestCase):
    def test_realistic_kept_and_deleted(self):
        for kept in (True, False):
            proof = keep.observation(frame(fields(kept)), TOKEN, CASE, kept, "12:34")
            self.assertEqual(proof["state_kept"], kept)
            self.assertEqual(proof["cgroup_kept"], kept)

    def test_ownership_kernel_and_resource_mismatches_fail(self):
        for field, value in (("owner", b"foreign\n"), ("root_identity", b"12:35"), ("fs", b"123"),
                             ("type", b"threaded\n"), ("procs", b"42\n"), ("events", b"populated 1\n"),
                             ("cpu", b"max 100000\n"), ("pids", b"max\n"), ("state_present", b"0"),
                             ("cgroup_present", b"0")):
            data = fields(True)
            data[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                keep.observation(frame(data), TOKEN, CASE, True, "12:34")

    def test_wrong_state_owner_and_live_pid_rejected(self):
        for key, value in (("id", "foreign"), ("status", "running"), ("pid", 42), ("bundle", "/other")):
            data = fields(True)
            state = json.loads(data["state"])
            state[key] = value
            data["state"] = json.dumps(state).encode()
            with self.subTest(key=key), self.assertRaises(ValueError):
                keep.observation(frame(data), TOKEN, CASE, True)
        data = fields(True)
        data["config"] = b'{"cgroup_path":"/foreign"}'
        with self.assertRaises(ValueError):
            keep.observation(frame(data), TOKEN, CASE, True)

    def test_partial_oversized_noncanonical_and_duplicate_frames_rejected(self):
        raw = frame(fields(True))
        for invalid in (raw[:-1], raw + b"\n", raw.replace(b"owner=", b"state="),
                        raw.replace(b"procs=\n", b"procs=!!!!\n"), raw + raw):
            with self.subTest(raw=invalid[:40]), self.assertRaises(ValueError):
                keep.observation(invalid, TOKEN, CASE, True)
        data = fields(True)
        data["state"] = b"x" * (keep.LIMIT + 1)
        with self.assertRaises(ValueError):
            keep.observation(frame(data), TOKEN, CASE, True)

    def test_no_partial_cleanup_accepted(self):
        for field in ("state_present", "cgroup_present", "state", "config", "procs"):
            data = fields(False)
            data[field] = b"1"
            with self.subTest(field=field), self.assertRaises(ValueError):
                keep.observation(frame(data), TOKEN, CASE, False)


class WorkerLogTests(unittest.TestCase):
    def test_absence_empty_and_complete_error_are_distinct(self):
        self.assertIsNone(keep.worker_log_bytes(b"absent\n"))
        self.assertEqual(keep.worker_log_bytes(b"present\n12:34:0:1:1\n\n"), b"")
        self.assertEqual(keep.worker_log_bytes(b"present\n12:34:3:1:1\neHl6\n"), b"xyz")

    def test_truncation_and_ambiguous_frames_rejected(self):
        for raw in (b"absent", b"absent\nextra\n", b"present\n12:34:4:1:1\neHl6\n",
                    b"present\n12:34:0:1:1\n", b"present\n12:34:3:1:1\n!!!!\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                keep.worker_log_bytes(raw)
        data = b"x" * (keep.LIMIT + 1)
        with self.assertRaises(ValueError):
            keep.worker_log_bytes(b"present\n12:34:32769:1:1\n" + base64.b64encode(data) + b"\n")

    def test_runtime_error_is_retained_then_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            builder = Mock(token=TOKEN, container_id="exact", descriptor={"owner": {}, "name": "context", "engine_id": "engine"},
                           inventory={"usr/bin/youki": {"sha256": "a" * 64}})
            builder.harness.evidence = Path(directory)
            builder.inspect_owned.return_value = {"State": {"Pid": 1, "StartedAt": "exact"}}
            builder.command.return_value = (b"present\n12:34:3:1:1\neHl6\n", b"", 0)
            with self.assertRaisesRegex(ValueError, "raw log retained"):
                keep.verify_worker_log(builder)
            self.assertEqual((Path(directory) / (TOKEN + "-post-workload-runtime-log.json")).read_bytes(), b"xyz")
            proof = json.loads((Path(directory) / (TOKEN + "-post-workload-runtime-log-proof.json")).read_bytes())
            self.assertFalse(proof["no_runtime_errors"])
            self.assertEqual(builder.inspect_owned.call_count, 2)

    def test_only_present_empty_log_passes_missing_is_retained_unproven(self):
        for raw, present in ((b"absent\n", False), (b"present\n12:34:0:1:1\n\n", True)):
            with self.subTest(present=present), tempfile.TemporaryDirectory() as directory:
                builder = Mock(token=TOKEN, container_id="exact", descriptor={"owner": {}, "name": "context", "engine_id": "engine"},
                               inventory={"usr/bin/youki": {"sha256": "a" * 64}})
                builder.harness.evidence = Path(directory)
                builder.inspect_owned.return_value = {"State": {"Pid": 1, "StartedAt": "exact"}}
                builder.command.return_value = (raw, b"", 0)
                if present:
                    proof = keep.verify_worker_log(builder)
                else:
                    with self.assertRaisesRegex(ValueError, "absence retained, success unproven"):
                        keep.verify_worker_log(builder)
                    proof = json.loads((Path(directory) / (TOKEN + "-post-workload-runtime-log-proof.json")).read_bytes())
                    self.assertIsNone(proof["size"])
                    self.assertIsNone(proof["sha256"])
                self.assertEqual(proof["present"], present)
                self.assertEqual(proof["no_runtime_errors"], present)
                self.assertEqual((Path(directory) / (TOKEN + "-post-workload-runtime-log.json")).exists(), present)

    def test_builder_lifetime_change_rejected(self):
        builder = Mock(container_id="exact")
        builder.inspect_owned.side_effect = [{"State": {"Pid": 1, "StartedAt": "a"}},
                                            {"State": {"Pid": 2, "StartedAt": "b"}}]
        builder.command.return_value = (b"absent\n", b"", 0)
        with self.assertRaisesRegex(ValueError, "lifetime changed"):
            keep.verify_worker_log(builder)

    def test_probe_is_bounded_regular_nonlink_and_read_only(self):
        self.assertIn('test ! -L "$p"', keep.WORKER_LOG_SCRIPT)
        self.assertIn('test -f "$p"', keep.WORKER_LOG_SCRIPT)
        self.assertIn('test "$before" = "$after"', keep.WORKER_LOG_SCRIPT)
        self.assertIn('head -c 32769', keep.WORKER_LOG_SCRIPT)
        for forbidden in ("rm ", "truncate ", "kill ", "mount ", "youki run"):
            self.assertNotIn(forbidden, keep.WORKER_LOG_SCRIPT)


class CachedWorkerLogTests(unittest.TestCase):
    def builder(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        builder = Mock(token=TOKEN, container_id="exact", role="importer", prepared=True, invocations=[],
                       descriptor={"owner": {"machine_id": "exact"}, "name": "context", "engine_id": "engine"},
                       inventory={"usr/bin/youki": {"sha256": "a" * 64}}, mapping={"container_id": "exact"})
        builder.harness.evidence = Path(temporary.name)
        builder.verify.return_value = {"role": "importer", "builder": builder.mapping,
                                       "owner": builder.descriptor["owner"], "youki_invocations": []}
        builder.inspect_owned.return_value = {"State": {"Pid": 1, "StartedAt": "exact"}}
        builder.command.return_value = (b"absent\n", b"", 0)
        return builder

    def test_absence_and_fresh_empty_history_are_scoped_not_workload_success(self):
        builder = self.builder()
        proof = keep.verify_cached_worker_log(builder)
        builder.verify.assert_called_once_with(require_invocation=False)
        self.assertEqual(builder.inspect_owned.call_count, 2)
        builder.command.assert_called_once_with("cached-worker-runtime-log",
            ["exec", "exact", "/bin/busybox", "sh", "-c", keep.WORKER_LOG_SCRIPT], timeout=15)
        self.assertTrue(proof["no_worker_execution_observed"])
        self.assertFalse(proof["present"])
        self.assertNotIn("no_runtime_errors", proof)
        self.assertIsNone(proof["size"])
        self.assertIsNone(proof["sha256"])
        self.assertEqual(proof["fresh_runtime_verification"], builder.verify.return_value)
        path = builder.harness.evidence / (TOKEN + "-cached-worker-runtime-log-proof.json")
        self.assertEqual(json.loads(path.read_bytes()), proof)
        self.assertFalse((builder.harness.evidence / (TOKEN + "-cached-worker-runtime-log.json")).exists())

    def test_even_present_empty_log_is_retained_and_rejected(self):
        for raw, content in ((b"present\n12:34:0:1:1\n\n", b""),
                             (b"present\n12:34:3:1:1\neHl6\n", b"xyz")):
            with self.subTest(content=content):
                builder = self.builder()
                builder.command.return_value = (raw, b"", 0)
                with self.assertRaisesRegex(ValueError, "unexplained worker runtime log"):
                    keep.verify_cached_worker_log(builder)
                prefix = builder.harness.evidence / (TOKEN + "-cached-worker-runtime-log.json")
                self.assertEqual(prefix.read_bytes(), content)
                proof = json.loads((builder.harness.evidence / (TOKEN + "-cached-worker-runtime-log-proof.json")).read_bytes())
                self.assertFalse(proof["no_worker_execution_observed"])

    def test_wrong_role_or_unprepared_fails_before_observation(self):
        for key, value in (("role", "source"), ("role", "cold-control"), ("prepared", False), ("prepared", 1)):
            builder = self.builder()
            setattr(builder, key, value)
            with self.assertRaisesRegex(ValueError, "prepared importer"):
                keep.verify_cached_worker_log(builder)
            builder.verify.assert_not_called()
            builder.command.assert_not_called()

    def test_fresh_runtime_history_and_identity_cannot_be_asserted_empty(self):
        for key, value in (("youki_invocations", ["vz-youki-invocation pid=2"]),
                           ("role", "source"), ("owner", {}), ("builder", {})):
            builder = self.builder()
            builder.verify.return_value[key] = value
            with self.assertRaisesRegex(ValueError, "invocation history"):
                keep.verify_cached_worker_log(builder)
            builder.command.assert_not_called()
        builder = self.builder()
        builder.invocations = ["vz-youki-invocation pid=2"]
        with self.assertRaisesRegex(ValueError, "invocation history"):
            keep.verify_cached_worker_log(builder)
        builder.command.assert_not_called()

    def test_replaced_lifetime_stderr_and_partial_capture_fail(self):
        for changes in ("lifetime", "stderr", "partial", "verification"):
            builder = self.builder()
            if changes == "lifetime":
                builder.inspect_owned.side_effect = [{"State": {"Pid": 1, "StartedAt": "exact"}},
                                                    {"State": {"Pid": 2, "StartedAt": "later"}}]
            elif changes == "stderr":
                builder.command.return_value = (b"absent\n", b"warning\n", 0)
            elif changes == "partial":
                builder.command.return_value = (b"absent", b"", 0)
            else:
                builder.verify.side_effect = ValueError("inventory rejected")
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                keep.verify_cached_worker_log(builder)
            self.assertEqual(list(builder.harness.evidence.iterdir()), [])


class IntegrationTests(unittest.TestCase):
    def test_keep_failure_withholds_cleanup_and_success_marks_verified(self):
        for fails in (True, False):
            h = gate.ComposeHarness.__new__(gate.ComposeHarness)
            h.info, h.builders, h.keep_proofs_verified = {"suite": "build"}, [], []
            h.builder_by_owner_role = {}
            descriptor = {"owner": {"project_id": "owned-project", "environment_id": "owned-environment",
                                    "machine_id": "owned-machine"}}
            builder = Mock()
            module = types.SimpleNamespace(Builder=Mock(return_value=builder))
            def verify(selected):
                self.assertEqual(h.keep_proofs_verified, [False])
                self.assertIs(selected, builder)
                if fails:
                    raise ValueError("keep probe failed")
            with patch.dict("sys.modules", {"linux_docker_buildkit_builder": module}), \
                    patch.object(gate, "input_mapping", return_value={}), patch.object(keep, "run", side_effect=verify):
                if fails:
                    with self.assertRaisesRegex(ValueError, "keep probe failed"):
                        h.driver_inputs(descriptor, {}, {}, {})
                    with self.assertRaisesRegex(Exception, "keep fixture"):
                        h.assert_certain()
                else:
                    h.driver_inputs(descriptor, {}, {}, {})
            self.assertEqual(h.keep_proofs_verified, [not fails])


if __name__ == "__main__":
    unittest.main()
