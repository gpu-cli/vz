"""Offline pinned-input and ownership tests; not BuildKit execution evidence."""
import copy
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_buildkit_builder as builder


def elf(kind=1, machine=183):
    result = bytearray(120)
    result[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<HH", result, 16, 2, machine)
    struct.pack_into("<Q", result, 32, 64)
    struct.pack_into("<HH", result, 54, 56, 1)
    struct.pack_into("<I", result, 64, kind)
    return bytes(result)


def archive_fixture(extra=False, link=False):
    binary = elf()
    contract = {"archive_sha256": "", "inventory": ["manifest.json", "bin/buildctl", "bin/buildkitd"],
                "buildkit_version": "fixture", "layout": 2, "platform": "linux/arm64", "source_commit": "a" * 40,
                "buildctl_sha256": builder.sha(binary), "buildkitd_sha256": builder.sha(binary)}
    manifest = {"buildkit": "fixture", "layout": 2, "platform": "linux/arm64", "source_commit": "a" * 40,
                "binaries": {name: builder.sha(binary) for name in ("buildctl", "buildkitd")}}
    files = {"manifest.json": json.dumps(manifest).encode(), "bin/buildctl": binary, "bin/buildkitd": binary}
    if extra:
        files["bin/runc"] = binary
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in files.items():
            entry = tarfile.TarInfo(name)
            entry.mode, entry.size = (0o644 if name == "manifest.json" else 0o755), len(data)
            if link and name == "bin/buildctl":
                entry.type, entry.linkname, entry.size = tarfile.SYMTYPE, "buildkitd", 0
            archive.addfile(entry, io.BytesIO(data))
    raw = stream.getvalue()
    contract["archive_sha256"] = builder.sha(raw)
    return raw, contract


class ArchiveTests(unittest.TestCase):
    def test_exact_runtime_free_archive(self):
        raw, contract = archive_fixture()
        self.assertEqual(set(builder.archive_members(raw, contract)), set(contract["inventory"]))

    def test_tampered_archive_binary_and_manifest_pins_rejected(self):
        raw, contract = archive_fixture()
        with self.assertRaises(ValueError):
            builder.archive_members(raw + b"bad", contract)
        for key in ("source_commit", "buildkit_version", "buildctl_sha256"):
            with self.assertRaises(ValueError):
                builder.archive_members(raw, dict(contract, **{key: "wrong"}))

    def test_extra_runtime_and_symlink_rejected(self):
        for options in ({"extra": True}, {"link": True}):
            raw, contract = archive_fixture(**options)
            with self.assertRaises(ValueError):
                builder.archive_members(raw, contract)

    def test_dynamic_foreign_and_truncated_binary_rejected(self):
        for raw in (elf(kind=2), elf(kind=3), elf(machine=62), elf()[:-1]):
            with self.assertRaises(ValueError):
                builder.static_arm64(raw)

    def test_preflight_uses_checked_in_contract_not_caller_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            raw, contract = archive_fixture()
            (root / "artifact.tar").write_bytes(raw)
            (root / "contract.json").write_text(json.dumps(contract))
            with patch.object(builder, "CONTRACT", root / "contract.json"):
                result = builder.preflight_archive(root / "artifact.tar")
                self.assertEqual(result["sha256"], contract["archive_sha256"])
                self.assertEqual(result["archive"], str(root / "artifact.tar"))
                (root / "artifact.tar").write_bytes(raw + b"bad")
                with self.assertRaises(ValueError):
                    builder.preflight_archive(root / "artifact.tar")


class Harness:
    def __init__(self, root):
        self.root, self.evidence = root, root / "evidence"
        self.evidence.mkdir()
        self.info = {"run_id": "builder-fixture", "buildkit": {"archive": "fixture"}}
        self.calls, self.objects, self.lines = [], {}, []
        self.b = None
        self.running = self.registered = self.uncertain = False
        self.keep_registration = False
        self.failure = self.forbidden = None
        self.engine_id = "engine-exact"

    def assert_certain(self):
        if self.uncertain:
            raise ValueError("uncertain host effect")

    def mutate(self, label, descriptor, args, **kwargs):
        self.calls.append((True, label, copy.deepcopy(descriptor), list(args)))
        if label == self.failure:
            self.uncertain = True
            raise ValueError("injected mutation failure")
        b = self.b
        if args[:2] == ["image", "import"]:
            self.objects["image"] = {"Id": "sha256:" + "a" * 64, "Architecture": "arm64", "Os": "linux",
                "Config": {"Labels": {builder.LABEL: b.token}, "Entrypoint": ["/usr/bin/buildkitd"],
                           "Env": list(builder.ENV)}}
            return ("sha256:" + "a" * 64 + "\n").encode(), b"", 0
        if args[:2] == ["volume", "create"]:
            self.objects["volume"] = {"Name": b.volume_name, "Driver": "local", "Scope": "local",
                "Labels": {builder.LABEL: b.token}, "Options": None, "CreatedAt": "2026-09-06T00:00:00Z",
                "Mountpoint": "/var/lib/docker/engine/volumes/" + b.volume_name + "/_data"}
        if args[:2] == ["container", "create"]:
            self.objects["container"] = {"Id": "b" * 64, "Name": "/" + b.container_name, "Image": b.image_id,
                "Config": {"Labels": {builder.LABEL: b.token}, "Entrypoint": ["/usr/bin/buildkitd"],
                           "Cmd": list(builder.FLAGS), "Env": list(builder.ENV)},
                "HostConfig": {"Runtime": "youki", "Privileged": True, "Init": True, "NetworkMode": "bridge",
                    "Binds": None, "PortBindings": {}, "RestartPolicy": {"Name": "no"}, "CgroupnsMode": "private"},
                "Mounts": [{"Type": "volume", "Name": b.volume_name, "Destination": "/var/lib/buildkit",
                    "Source": self.objects["volume"]["Mountpoint"], "RW": True}],
                "State": {"Running": False, "Status": "created", "ExitCode": 0,
                          "Paused": False, "Restarting": False, "Dead": False,
                          "Pid": 737, "StartedAt": "2026-09-06T00:00:01.123456789Z"}, "RestartCount": 0}
            return ("b" * 64 + "\n").encode(), b"", 0
        if args[:2] in (["container", "start"], ["container", "stop"]):
            running = args[1] == "start"
            self.objects["container"]["State"].update(Running=running, Status="running" if running else "exited")
        if args[:2] == ["buildx", "create"]:
            self.registered = True
        if args[:2] == ["buildx", "rm"]:
            self.registered = self.keep_registration
        if len(args) > 1 and args[1] == "rm":
            self.objects.pop(args[0], None)
        if args[:2] == ["buildx", "inspect"]:
            return self.inspect_text(), b"", 0
        return b"", b"", 0

    def inspect_text(self):
        return f"Name: {self.b.name}\nDriver: docker-container\n\nNodes:\nName: {self.b.node}\nEndpoint: exact-context\nStatus: running\n".encode()

    def docker(self, label, descriptor, args, **kwargs):
        self.calls.append((False, label, copy.deepcopy(descriptor), list(args)))
        if args[0] == "info":
            value = {"ID": self.engine_id, "OSType": "linux", "Architecture": "aarch64", "DefaultRuntime": "youki",
                     "Runtimes": {"youki": {"path": "/mnt/linux-bin/youki"}}}
            return json.dumps(value).encode(), b"", 0
        if args[:2] == ["buildx", "ls"]:
            return (self.b.name.encode() if self.registered else b""), b"", 0
        if args[:2] == ["buildx", "inspect"]:
            return self.inspect_text(), b"", 0
        if len(args) > 1 and args[1] == "inspect":
            return json.dumps([self.objects[args[0]]]).encode(), b"", 0
        if len(args) > 1 and args[1] == "ls":
            return (b"present" if args[0] in self.objects else b""), b"", 0
        if label == "builder-forbidden-runtime-inventory":
            return (self.forbidden or b""), b"", 0
        if label == "builder-runtime-hashes":
            return "".join(f"{value['sha256']}  /{name}\n" for name, value in self.b.inventory.items()).encode(), b"", 0
        if label == "builder-workers":
            return b"ID:\tworker-exact\nLabels:\n\torg.mobyproject.buildkit.worker.executor:\toci\n", b"", 0
        if label == "builder-youki-invocations":
            return "\n".join(self.lines).encode(), b"", 0
        raise AssertionError((label, args))


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-builder-owner-test-")
        self.addCleanup(self.temp.cleanup)
        self.harness = Harness(Path(self.temp.name).resolve())
        self.descriptor = {"name": "exact-context", "engine_id": "engine-exact",
                           "owner": {"project_id": "prj_a", "environment_id": "env_a", "machine_id": "mch_a"}}
        self.b = builder.Builder(self.harness, self.descriptor)
        self.harness.b = self.b
        self.payload = patch.object(builder, "rootfs_payload", return_value=(b"fixture rootfs",
            {"usr/bin/youki": {"sha256": "c" * 64, "mode": 0o755, "size": 1}}))
        self.payload.start()
        self.addCleanup(self.payload.stop)
        self.cgroup = patch.object(builder.cgroup, "capture", return_value={"offline_test_only": True})
        self.capture = self.cgroup.start()
        self.addCleanup(self.cgroup.stop)

    def test_exact_precreated_builder_has_no_pull_default_or_runtime_fallback(self):
        mapping = self.b.prepare()
        self.assertEqual(mapping["container_id"], "b" * 64)
        commands = [args for _, _, _, args in self.harness.calls]
        create_container = next(i for i, args in enumerate(commands) if args[:2] == ["container", "create"])
        register = next(i for i, args in enumerate(commands) if args[:2] == ["buildx", "create"])
        self.assertLess(create_container, register)
        imported = next(args for args in commands if args[:2] == ["image", "import"])
        self.assertIn("ENV BUILDKIT_SETUP_CGROUPV2_ROOT=1", imported)
        self.assertEqual(commands[create_container][commands[create_container].index("--cgroupns") + 1], "private")
        proof = json.loads((self.harness.evidence / (self.b.token + "-image-input.json")).read_bytes())
        retained = self.harness.evidence / proof["rootfs_archive"]
        self.assertEqual(retained.read_bytes(), b"fixture rootfs")
        self.assertEqual(builder.startup.digest(retained), proof["rootfs_sha256"])
        self.assertEqual(proof["image_env"], builder.ENV)
        self.assertEqual(proof["cgroup_namespace"], "private")
        self.assertTrue(all(scope == self.descriptor for _, _, scope, _ in self.harness.calls))
        self.capture.assert_called_once()
        self.assertEqual(self.capture.call_args.args[1], self.descriptor)
        for args in commands:
            self.assertNotIn("--use", args)
            self.assertNotIn("prune", args)
            self.assertNotIn("pull", args)
            self.assertNotIn("--runtime", args)

    def test_new_worker_invocation_is_required_after_workloads(self):
        self.b.prepare()
        with self.assertRaises(ValueError):
            self.b.verify()
        self.harness.lines = ["vz-youki-invocation pid=27"]
        self.assertTrue(self.b.verify()["post_workload"])
        with self.assertRaises(ValueError):
            self.b.verify()

    def test_cgroup_observation_failure_withholds_recipe_readiness(self):
        self.capture.side_effect = ValueError("builder cgroup: root still contains processes")
        with self.assertRaisesRegex(ValueError, "root still contains processes"):
            self.b.prepare()
        self.assertFalse(any(label == "builder-runtime-hashes" for _, label, *_ in self.harness.calls))

    def test_bootstrap_failure_observed_externally_without_retry_or_cleanup(self):
        self.harness.failure = "builder-bootstrap"
        self.capture.side_effect = ValueError("external observation failed")
        with self.assertRaisesRegex(ValueError, "injected mutation failure"):
            self.b.prepare()
        self.assertEqual(sum(label == "builder-bootstrap" for _, label, *_ in self.harness.calls), 1)
        self.assertFalse(any(args[1:2] == ["rm"] for _, _, _, args in self.harness.calls))
        self.assertEqual(json.loads((self.harness.evidence / (self.b.token + "-bootstrap-failure-cgroup.json")).read_bytes()),
                         {"observation_failed": "external observation failed"})

    def test_missing_conflicting_or_duplicate_setup_environment_rejected(self):
        self.b.prepare()
        for kind in ("image", "container"):
            config = self.harness.objects[kind]["Config"]
            for env in (None, builder.ENV[:-1], builder.ENV[:-1] + ["BUILDKIT_SETUP_CGROUPV2_ROOT=0"],
                        builder.ENV + ["BUILDKIT_SETUP_CGROUPV2_ROOT=1"],
                        builder.ENV + ["BUILDKIT_SETUP_CGROUPV2_ROOT=0"]):
                with self.subTest(kind=kind, env=env):
                    config["Env"] = env
                    before = len(self.harness.calls)
                    with self.assertRaisesRegex(ValueError, "configuration drift"):
                        self.b.remove_owned()
                    self.assertFalse(any(mutation for mutation, *_ in self.harness.calls[before:]))
            config["Env"] = list(builder.ENV)

    def test_nonprivate_cgroup_namespace_rejected_without_mutation(self):
        self.b.prepare()
        host = self.harness.objects["container"]["HostConfig"]
        for namespace in (None, "", "host"):
            host["CgroupnsMode"] = namespace
            before = len(self.harness.calls)
            with self.assertRaisesRegex(ValueError, "policy drift"):
                self.b.remove_owned()
            self.assertFalse(any(mutation for mutation, *_ in self.harness.calls[before:]))

    def test_live_builder_incarnation_drift_rejected(self):
        self.b.prepare()
        state = self.harness.objects["container"]["State"]
        original = dict(state)
        for key, value in (("Pid", True), ("Pid", 0), ("Pid", 738), ("StartedAt", None),
                           ("Paused", True), ("Restarting", True), ("Dead", True),
                           ("StartedAt", "2026-99-06T99:99:99Z"),
                           ("StartedAt", "0001-01-01T00:00:00Z"),
                           ("StartedAt", "2026-09-06T00:00:02.123456789Z")):
            with self.subTest(key=key, value=value):
                state.update(original)
                state[key] = value
                with self.assertRaises(ValueError):
                    self.b.verify(False)
        state.update(original)
        self.harness.objects["container"]["RestartCount"] = False
        with self.assertRaisesRegex(ValueError, "lifecycle drift"):
            self.b.remove_owned()
    def test_foreign_engine_or_cache_volume_fails_before_mutation(self):
        self.harness.engine_id = "foreign"
        with self.assertRaises(ValueError):
            self.b.prepare()
        self.assertFalse(any(mutation for mutation, *_ in self.harness.calls))
        self.harness.engine_id = "engine-exact"
        self.b.prepare()
        self.harness.objects["volume"]["Labels"] = {builder.LABEL: "foreign"}
        before = len(self.harness.calls)
        with self.assertRaises(ValueError):
            self.b.remove_owned()
        self.assertFalse(any(mutation for mutation, *_ in self.harness.calls[before:]))

    def test_runtime_or_extra_mount_drift_and_forbidden_cache_binary_rejected(self):
        self.b.prepare()
        item = self.harness.objects["container"]
        item["HostConfig"]["Runtime"] = "runc"
        with self.assertRaises(ValueError):
            self.b.verify(False)
        item["HostConfig"]["Runtime"] = "youki"
        item["Mounts"].append(dict(item["Mounts"][0]))
        with self.assertRaises(ValueError):
            self.b.verify(False)
        item["Mounts"].pop()
        self.harness.forbidden = b"/var/lib/buildkit/snapshots/forbidden/runc\n"
        with self.assertRaises(ValueError):
            self.b.verify(False)

    def test_partial_or_uncertain_provisioning_is_retained(self):
        self.harness.failure = "builder-volume-create"
        with self.assertRaises(ValueError):
            self.b.prepare()
        before = len(self.harness.calls)
        with self.assertRaises(ValueError):
            self.b.remove_owned()
        self.assertEqual(before, len(self.harness.calls))
        self.assertIn("image", self.harness.objects)
        with self.assertRaises(ValueError):
            self.b.prepare()

    def test_cleanup_unregisters_without_implicit_deletion_then_removes_exact_objects(self):
        self.b.prepare()
        self.b.remove_owned()
        mutations = [args for mutation, _, _, args in self.harness.calls if mutation]
        self.assertIn(["buildx", "rm", "--keep-daemon", "--keep-state", self.b.name], mutations)
        self.assertIn(["container", "rm", self.b.container_id], mutations)
        self.assertIn(["volume", "rm", self.b.volume_name], mutations)
        self.assertIn(["image", "rm", self.b.tag], mutations)
        self.assertEqual(self.harness.objects, {})
        self.assertFalse(self.harness.registered)
        proof = json.loads((self.harness.evidence / (self.b.token + "-cleanup.json")).read_bytes())
        self.assertTrue(proof["buildx_registration_absent"])

    def test_remaining_registration_withholds_container_volume_image_deletion(self):
        self.b.prepare()
        self.harness.keep_registration = True
        before = len(self.harness.calls)
        with self.assertRaisesRegex(ValueError, "builder registration remains"):
            self.b.remove_owned()
        mutations = [args for mutation, _, _, args in self.harness.calls[before:] if mutation]
        self.assertEqual(mutations, [["buildx", "rm", "--keep-daemon", "--keep-state", self.b.name]])
        self.assertEqual(set(self.harness.objects), {"container", "volume", "image"})


if __name__ == "__main__":
    unittest.main()
