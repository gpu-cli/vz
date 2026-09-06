"""Offline public-Up/runtime-proof attribution tests, never runtime evidence."""

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import docker_host_driver as driver
import linux_docker_e2e as gate


def sha(data):
    return hashlib.sha256(data).hexdigest()


class ActivationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="vz-activation-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.h = SimpleNamespace(evidence=self.root / "evidence", runtime=self.root / "runtime",
                                 prefix=self.root / "install", config=self.root / "docker",
                                 record=SimpleNamespace(receipts=[]))
        for directory in (self.h.evidence, self.h.runtime, self.h.config / "cli-plugins",
                          self.h.prefix / "linux/developer"):
            directory.mkdir(parents=True)
            directory.chmod(0o700)
        self.owner = {"project_id": "prj_test", "environment_id": "env_test", "machine_id": "mch_test"}
        self.machine_config = gate.startup.machine_config_path(self.h.runtime, self.owner)
        self.machine_config.mkdir(parents=True, mode=0o700)
        self.machine_config.chmod(0o700)
        metadata = self.machine_config.stat()
        self.write(self.machine_config.parent.parent / 'owner.json', json.dumps({'owner': self.owner}).encode())
        self.write(self.machine_config / 'vz-owner.json', json.dumps({'schema_version': 1, 'owner': self.owner,
            'directory': {'device': metadata.st_dev, 'inode': metadata.st_ino}, 'nonce': 'lop_' + 'a' * 32}).encode())
        self.incarnation = {"schema_version": 1, "machine_id": "mch_test", "incarnation_id":
                            "inc_runtime_11111111-2222-4333-8444-555555555555", "generation": 1, "created_at": 100}
        self.identity = {"schema_version": 1, "opaque_id": json.dumps({"schema_version": 1,
                         "stack_id": "vzr1-other-runtime_vm-vm-" + "a" * 32,
                         "incarnation_id": "11111111-2222-4333-8444-555555555555"}, separators=(",", ":"))}
        self.descriptor = {"schema_version": 1, "owner": self.owner, "name": "vzr1-private-test",
                           "endpoint": "unix://" + str(self.root / "machine.sock"), "config_dir": str(self.machine_config),
                           "engine_id": "11111111-aaaa-4bbb-8ccc-222222222222", "incarnation_id": self.incarnation["incarnation_id"],
                           "incarnation_generation": 1}
        self.capabilities = {"capabilities": ["posix_exec", "docker_engine", "compose", "buildx"]}
        self.machine = {"machine_id": "mch_test", "name": "machine-0", "state": "ready", "backend": "macos_virtualization_linux",
                        "profile": "developer", "target": {"os": "linux", "arch": "aarch64", "image": "vz-linux-appliance",
                        "digest": "sha256:" + "d" * 64}, "docker_context": self.descriptor,
                        "requested_capabilities": {"capabilities": []},
                        "negotiated_capabilities": self.capabilities, "incarnation_id": self.incarnation["incarnation_id"],
                        "incarnation_generation": 1}
        self.environment = {"project_id": "prj_test", "environment_id": "env_test", "state": "ready",
                            "definition_digest": "sha256:" + "e" * 64, "lifecycle_generation": 1,
                            "machines": [self.machine]}
        activation = {"schema_version": 1, "backend": self.machine["backend"], "docker_context": self.descriptor,
                      "incarnation": self.incarnation, "runtime_identity": self.identity,
                      "negotiated_capabilities": self.capabilities}
        common = {"schema_version": 1, "project_id": "prj_test", "environment_id": "env_test",
                  "request_id": "request-test", "idempotency_key": "idempotency-test",
                  "request_hash": "sha256:" + "f" * 64, "definition_digest": self.environment["definition_digest"]}
        self.completion = {"error": None, "admission": common | {"machine_ids": ["mch_test"]},
                           "operation": common | {"operation_id": "lop_test", "kind": "up", "status": "succeeded",
                           "generation": 1, "machine_steps": [{"machine_id": "mch_test", "status": "succeeded",
                           "target_state": "ready", "resulting_incarnation": self.incarnation,
                           "resulting_activation": activation}]}}
        self.command = {"index": 1, "label": "public-up", "argv": [str(self.h.prefix / "bin/vz"), "--json", "up",
                        "--environment", "env_test", "--request-id", "request-test", "--idempotency-key", "idempotency-test"],
                        "capture_complete": True, "effects_uncertain": False, "exit_code": 0}
        self.capture(self.completion)
        client = self.root / "docker-client"
        self.write(client, b"offline synthetic executable", 0o500)
        for name in ("compose", "buildx"):
            self.write(self.h.config / "cli-plugins" / ("docker-" + name), b"offline plugin", 0o500)
        self.h.info = {"run_id": "activation-test", "fixture_sha256": "a" * 64, "release_version": "0.4.0-dev",
                       "clients": {"docker": {"canonical": str(client), "sha256": sha(client.read_bytes())},
                                   "vz": {"sha256": "b" * 64}},
                       "python_image": {"reference": "docker.io/library/python@sha256:" + "c" * 64,
                                        "id": "sha256:" + "d" * 64, "platform": "linux/arm64"}}
        self.h.catalog = {"linux": [{"profile": "developer", "image": self.machine["target"]["image"],
                                      "digest": self.machine["target"]["digest"]}]}
        self.write(self.h.prefix / "linux/developer/youki", b"pinned youki")
        self.write(self.h.prefix / "linux/developer/developer-probe-rootfs.tar", b"pinned archive")
        self.write(self.h.prefix / "linux/developer/vmlinux", b"pinned kernel")
        self.write(self.h.prefix / "linux/developer/initramfs.img", b"pinned initramfs")
        self.write(self.h.prefix / "linux/developer/version.json", b'{"profile":"developer"}')
        self.attempt = self.h.runtime / "store/data/attempt"
        self.attempt.mkdir(mode=0o700, parents=True)
        self.configuration_path = self.attempt.parent / "linux-target/configuration.json"
        self.configuration_path.parent.mkdir(mode=0o700)
        self.configuration = {"schema_version": 1, "host": {"os": "macos", "arch": "aarch64"},
            "backend": self.machine["backend"], "kernel_profile": "developer", "release_version": "0.4.0-dev",
            "resources": {"cpus": 2, "memory_mb": 4096},
            "machine": {"schema_version": 1, "name": "machine-0", "profile": "developer",
                        "target": copy.deepcopy(self.machine["target"]), "requested_capabilities": {"capabilities": []},
                        "resources": {"cpus": 2, "memory_mb": 4096}},
            "artifact": {"digest": self.machine["target"]["digest"], "youki_sha256": sha(b"pinned youki"),
                         "kernel_sha256": sha(b"pinned kernel"), "initramfs_sha256": sha(b"pinned initramfs"),
                         "version_sha256": sha(b'{"profile":"developer"}')}}
        config_raw = json.dumps(self.configuration, sort_keys=True, separators=(",", ":")).encode()
        self.write(self.configuration_path, config_raw, 0o400)
        self.configuration_digest = "sha256:" + sha(b"vz.machine-configuration.v1\0" + config_raw)
        self.owner_manifest = {"schema_version": 1, "owner": self.owner,
                              "configuration_digest": self.configuration_digest,
                              "reservation": {"schema_version": 1, "resource_kind": {"other": "machine_runtime_store"},
                                  "resource_id": "store", "environment_id": self.owner["environment_id"],
                                  "machine_id": self.owner["machine_id"]}}
        self.owner_path = self.attempt.parent.parent / "owner.json"
        self.write(self.owner_path, json.dumps(self.owner_manifest).encode())
        youki_sha = sha(b"pinned youki")
        inventory = {"owner": self.owner, "incarnation": self.incarnation, "youki_sha256": youki_sha,
                     "scope": "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
                     "stdout": "vz-startup-runtime-inventory-v1\nyouki-sha256=" + youki_sha +
                               "\nyouki version: offline-fixture\nalternate-runtime-binaries=absent\n"}
        self.receipt = {"schema_version": 1, "state": "completed", "failure": None, "owner": self.owner,
                        "configuration_digest": self.configuration_digest,
                        "incarnation": self.incarnation, "archive_sha256": sha(b"pinned archive"),
                        "context": self.descriptor["name"], "client_sha256": self.h.info["clients"]["docker"]["sha256"],
                        "resources": {"engine_id": self.descriptor["engine_id"],
                        "cleanup_scope": "disposable_probe_containers_compose_objects_and_images",
                        "retained_buildkit_cache": True, "runtime_inventory": inventory}}
        self.proof()

    @staticmethod
    def write(path, data, mode=0o600):
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(data)
        path.chmod(mode)

    def capture(self, completion, copies=1):
        raw = (json.dumps({"progress": {"completion": completion}}).encode() + b"\n") * copies
        self.write(self.h.evidence / "001-public-up.stdout", raw)
        self.command["stdout_sha256"] = sha(raw)
        self.h.record.receipts = [self.command]

    def capture_history(self, completions):
        self.h.record.receipts = []
        for index, completion in enumerate(completions, 1):
            raw = json.dumps({"progress": {"completion": completion}}).encode() + b"\n"
            self.write(self.h.evidence / f"{index:03}-public-up.stdout", raw)
            command = copy.deepcopy(self.command)
            command.update(index=index, stdout_sha256=sha(raw))
            command["argv"][command["argv"].index("--request-id") + 1] = completion["admission"]["request_id"]
            command["argv"][command["argv"].index("--idempotency-key") + 1] = completion["admission"]["idempotency_key"]
            self.h.record.receipts.append(command)

    def proof(self):
        raw = json.dumps(self.receipt).encode()
        self.write(self.attempt / "receipt.json", raw)
        after = {"schema_version": 1, "probe_receipt_sha256": sha(raw),
                 "runtime_inventory": self.receipt["resources"]["runtime_inventory"]}
        self.write(self.attempt / "runtime-inventory-after.json", json.dumps(after).encode())

    def reject_activation(self, completion):
        self.capture(completion)
        with self.assertRaises((driver.Rejected, KeyError, ValueError)):
            gate.public_activation(self.h, self.environment, self.machine)

    def test_exact_public_identity_not_configuration_hash(self):
        self.assertEqual(gate.public_activation(self.h, self.environment, self.machine), self.identity)
        scope, proof = gate.authenticated_proof(self.h, self.environment, self.machine)
        self.assertEqual(scope["runtime_identity"], self.identity["opaque_id"])
        self.assertEqual(proof["receipt_sha256"], sha((self.attempt / "receipt.json").read_bytes()))
        binding = json.loads((self.h.evidence / "mch_test-runtime-binding.json").read_bytes())
        self.assertEqual(binding["runtime_identity_material"], self.identity)

    def test_private_client_receipt_names_are_never_read_as_runtime_proofs(self):
        canary = b'public-private-credential-canary-not-json'
        for selected in (self.machine_config, self.machine_config.with_name('.docker-client.pending-owned')):
            nested = selected / 'private-plugin'
            nested.mkdir(parents=True)
            self.write(nested / 'receipt.json', canary)
        reader = gate.startup.read_private_regular
        def read(path, limit):
            # The exact public ownership claim is intentionally admitted before
            # proof discovery; arbitrary private client receipts are not.
            if path.is_relative_to(self.h.runtime) and gate.startup.is_private_client_path(path, self.h.runtime):
                self.assertEqual(path, self.machine_config / 'vz-owner.json')
            return reader(path, limit)
        with patch.object(gate.startup, 'read_private_regular', side_effect=read):
            scope, proof = gate.authenticated_proof(self.h, self.environment, self.machine)
        self.assertEqual(proof['receipt_sha256'], sha((self.attempt / 'receipt.json').read_bytes()))
        binding = (self.h.evidence / 'mch_test-runtime-binding.json').read_bytes()
        self.assertNotIn(canary, binding)

    def test_capture_tamper_nonzero_uncertain_and_incomplete_rejected(self):
        for field, changed in (("stdout_sha256", "0" * 64), ("exit_code", 1),
                               ("effects_uncertain", True), ("capture_complete", False)):
            with self.subTest(field=field):
                original = self.command[field]
                self.command[field] = changed
                with self.assertRaises(driver.Rejected):
                    gate.public_activation(self.h, self.environment, self.machine)
                self.command[field] = original

    def test_scope_and_terminal_mismatch_rejected(self):
        for section, field, changed in (("admission", "request_id", "foreign"),
                ("admission", "idempotency_key", "foreign"), ("admission", "project_id", "foreign"),
                ("admission", "environment_id", "foreign"), ("admission", "request_hash", "foreign"),
                ("operation", "status", "running"), ("operation", "kind", "stop")):
            with self.subTest(section=section, field=field):
                completion = copy.deepcopy(self.completion)
                completion[section][field] = changed
                self.reject_activation(completion)
        self.reject_activation(self.completion | {"error": {"code": "failed"}})

    def test_current_definition_and_lifecycle_generation_required(self):
        for changed in ("definition", "generation"):
            with self.subTest(changed=changed):
                completion = copy.deepcopy(self.completion)
                if changed == "definition":
                    completion["admission"]["definition_digest"] = "sha256:" + "1" * 64
                    completion["operation"]["definition_digest"] = "sha256:" + "1" * 64
                else:
                    completion["operation"]["generation"] = 2
                self.reject_activation(completion)

    def test_reup_selects_current_generation_and_preserves_earlier_raw_capture(self):
        historical = copy.deepcopy(self.completion)
        # Earlier topology definitions/incarnations are not current authority.
        for section in ("admission", "operation"):
            historical[section]["definition_digest"] = "sha256:" + "1" * 64
        new_uuid = "66666666-2222-4333-8444-555555555555"
        self.incarnation.update(incarnation_id="inc_runtime_" + new_uuid, generation=2)
        self.machine.update(incarnation_id=self.incarnation["incarnation_id"], incarnation_generation=2)
        self.descriptor.update(incarnation_id=self.incarnation["incarnation_id"], incarnation_generation=2)
        runtime = json.loads(self.identity["opaque_id"])
        runtime["incarnation_id"] = new_uuid
        self.identity["opaque_id"] = json.dumps(runtime, separators=(",", ":"))
        current = copy.deepcopy(self.completion)
        self.environment["lifecycle_generation"] = 3
        current["operation"]["generation"] = 3
        for section in ("admission", "operation"):
            current[section]["request_id"] = "reup-request"
            current[section]["idempotency_key"] = "reup-key"
        for completions in ([historical, current], [current, historical]):
            with self.subTest(current_first=completions[0] is current):
                self.capture_history(completions)
                before = {path: path.read_bytes() for path in self.h.evidence.glob("*-public-up.stdout")}
                receipts = copy.deepcopy(self.h.record.receipts)
                self.assertEqual(gate.public_activation(self.h, self.environment, self.machine), self.identity)
                self.assertEqual(self.h.record.receipts, receipts)
                self.assertEqual({path: path.read_bytes() for path in before}, before)

    def test_history_cannot_mask_corrupt_or_foreign_current_generation(self):
        historical = copy.deepcopy(self.completion)
        self.environment["lifecycle_generation"] = 3
        current = copy.deepcopy(self.completion)
        current["operation"]["generation"] = 3
        mutations = [
            lambda row: row["admission"].update(environment_id="env_foreign"),
            lambda row: row["operation"].update(environment_id="env_foreign"),
            lambda row: row["admission"].update(project_id="prj_foreign"),
            lambda row: row["operation"].update(definition_digest="sha256:" + "2" * 64),
            lambda row: row["operation"]["machine_steps"][0]["resulting_activation"]["runtime_identity"].update(opaque_id="foreign"),
            lambda row: row["operation"]["machine_steps"][0].update(status="failed"),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                corrupt = copy.deepcopy(current)
                mutate(corrupt)
                # A second valid current record must not hide the corrupt one.
                self.capture_history([historical, corrupt, current])
                with self.assertRaises((driver.Rejected, KeyError, ValueError)):
                    gate.public_activation(self.h, self.environment, self.machine)

    def test_history_rejects_invalid_future_and_missing_or_duplicate_current(self):
        self.environment["lifecycle_generation"] = 3
        current = copy.deepcopy(self.completion)
        current["operation"]["generation"] = 3
        for generation in (None, False, True, 0, -1, "1", 1.0, 4, 2**64):
            with self.subTest(generation=generation):
                invalid = copy.deepcopy(self.completion)
                invalid["operation"]["generation"] = generation
                self.capture_history([invalid, current])
                with self.assertRaises(driver.Rejected):
                    gate.public_activation(self.h, self.environment, self.machine)
        for completions in ([self.completion], [self.completion, current, current]):
            with self.subTest(count=len(completions)):
                self.capture_history(completions)
                with self.assertRaises(driver.Rejected):
                    gate.public_activation(self.h, self.environment, self.machine)

    def test_historical_capture_tamper_and_invalid_current_status_generation_reject(self):
        current = copy.deepcopy(self.completion)
        current["operation"]["generation"] = 3
        self.environment["lifecycle_generation"] = 3
        self.capture_history([self.completion, current])
        self.h.record.receipts[0]["stdout_sha256"] = "0" * 64
        with self.assertRaises(driver.Rejected):
            gate.public_activation(self.h, self.environment, self.machine)
        self.capture_history([self.completion, current])
        for generation in (None, False, True, 0, -1, "3", 3.0, 2**64):
            with self.subTest(generation=generation):
                self.environment["lifecycle_generation"] = generation
                with self.assertRaises(driver.Rejected):
                    gate.public_activation(self.h, self.environment, self.machine)

    def test_duplicate_terminal_and_machine_inventory_rejected(self):
        self.capture(self.completion, copies=2)
        with self.assertRaises(driver.Rejected):
            gate.public_activation(self.h, self.environment, self.machine)
        for changed in ("admission_duplicate", "extra_step", "duplicate_step", "missing_step"):
            with self.subTest(changed=changed):
                completion = copy.deepcopy(self.completion)
                steps = completion["operation"]["machine_steps"]
                if changed == "admission_duplicate":
                    completion["admission"]["machine_ids"].append("mch_test")
                elif changed == "extra_step":
                    steps.append(copy.deepcopy(steps[0]) | {"machine_id": "mch_foreign"})
                elif changed == "duplicate_step":
                    steps.append(copy.deepcopy(steps[0]))
                else:
                    steps.clear()
                self.reject_activation(completion)

    def test_activation_descriptor_backend_capability_and_incarnation_rejected(self):
        for changed in ("context", "backend", "capability", "incarnation", "step_status", "target"):
            with self.subTest(changed=changed):
                completion = copy.deepcopy(self.completion)
                step = completion["operation"]["machine_steps"][0]
                activation = step["resulting_activation"]
                if changed == "context":
                    activation["docker_context"]["engine_id"] = "foreign"
                elif changed == "backend":
                    activation["backend"] = "foreign"
                elif changed == "capability":
                    activation["negotiated_capabilities"]["capabilities"] = []
                elif changed == "incarnation":
                    activation["incarnation"]["generation"] = 2
                elif changed == "step_status":
                    step["status"] = "failed"
                else:
                    step["target_state"] = "stopped"
                self.reject_activation(completion)

    def test_runtime_identity_cannot_be_foreign_or_control_injected(self):
        for changed in ("uuid", "stack", "schema", "fields", "control"):
            completion = copy.deepcopy(self.completion)
            identity = completion["operation"]["machine_steps"][0]["resulting_activation"]["runtime_identity"]
            raw = json.loads(identity["opaque_id"])
            if changed == "uuid":
                raw["incarnation_id"] = "00000000-0000-4000-8000-000000000000"
            elif changed == "stack":
                raw["stack_id"] = "foreign"
            elif changed == "schema":
                raw["schema_version"] = 2
            elif changed == "fields":
                raw["unexpected"] = True
            identity["opaque_id"] = json.dumps(raw, separators=(",", ":")) + ("\n" if changed == "control" else "")
            self.reject_activation(completion)

    def test_wrong_receipt_owner_generation_archive_or_duplicate_is_rejected(self):
        original = copy.deepcopy(self.receipt)
        for changed in ("owner", "generation", "archive", "duplicate"):
            with self.subTest(changed=changed):
                self.receipt = copy.deepcopy(original)
                if changed == "owner":
                    self.receipt["owner"]["machine_id"] = "mch_foreign"
                elif changed == "generation":
                    self.receipt["incarnation"]["generation"] = 2
                elif changed == "archive":
                    self.receipt["archive_sha256"] = "0" * 64
                self.proof()
                duplicate = self.h.runtime / "duplicate"
                if changed == "duplicate":
                    duplicate.mkdir(mode=0o700)
                    self.write(duplicate / "receipt.json", (self.attempt / "receipt.json").read_bytes())
                with self.assertRaises(driver.Rejected):
                    gate.authenticated_proof(self.h, self.environment, self.machine)
                self.assertFalse((self.h.evidence / "mch_test-runtime-binding.json").exists())

    def test_inventory_tamper_or_incomplete_receipt_never_publishes_binding(self):
        original = copy.deepcopy(self.receipt)
        for changed in ("state", "client", "engine", "runtime", "cleanup", "after"):
            with self.subTest(changed=changed):
                self.receipt = copy.deepcopy(original)
                if changed == "state":
                    self.receipt["state"] = "failed_recovery_required"
                elif changed == "client":
                    self.receipt["client_sha256"] = "0" * 64
                elif changed == "engine":
                    self.receipt["resources"]["engine_id"] = "foreign"
                elif changed == "runtime":
                    self.receipt["resources"]["runtime_inventory"]["youki_sha256"] = "0" * 64
                elif changed == "cleanup":
                    self.receipt["resources"]["cleanup_scope"] = "nothing"
                self.proof()
                if changed == "after":
                    path = self.attempt / "runtime-inventory-after.json"
                    after = json.loads(path.read_bytes())
                    after["probe_receipt_sha256"] = "0" * 64
                    self.write(path, json.dumps(after).encode())
                with self.assertRaises(driver.Rejected):
                    gate.authenticated_proof(self.h, self.environment, self.machine)
                self.assertFalse((self.h.evidence / "mch_test-runtime-binding.json").exists())

    def test_configuration_uses_domain_hash_and_exact_owner_manifest(self):
        original_receipt = copy.deepcopy(self.receipt)
        original_owner = copy.deepcopy(self.owner_manifest)
        for changed in ("plain_sha", "owner", "owner_digest", "config_bytes"):
            with self.subTest(changed=changed):
                self.receipt = copy.deepcopy(original_receipt)
                owner = copy.deepcopy(original_owner)
                raw = json.dumps(self.configuration, sort_keys=True, separators=(",", ":")).encode()
                if changed == "plain_sha":
                    self.receipt["configuration_digest"] = "sha256:" + sha(raw)
                    owner["configuration_digest"] = self.receipt["configuration_digest"]
                elif changed == "owner":
                    owner["owner"]["environment_id"] = "env_foreign"
                elif changed == "owner_digest":
                    owner["configuration_digest"] = "sha256:" + "0" * 64
                else:
                    raw += b"\n"
                self.write(self.owner_path, json.dumps(owner).encode())
                self.write(self.configuration_path, raw, 0o400)
                self.proof()
                with self.assertRaises(driver.Rejected):
                    gate.authenticated_proof(self.h, self.environment, self.machine)
                self.assertFalse((self.h.evidence / "mch_test-runtime-binding.json").exists())

    def test_rehashed_configuration_cannot_substitute_catalog_or_installed_artifacts(self):
        original = copy.deepcopy(self.configuration)
        for changed in ("profile", "target", "artifact", "youki", "kernel", "initramfs", "version", "release", "name", "capabilities", "backend"):
            with self.subTest(changed=changed):
                config = copy.deepcopy(original)
                if changed == "profile":
                    config["kernel_profile"] = "container"
                elif changed == "target":
                    config["machine"]["target"]["digest"] = "sha256:" + "0" * 64
                elif changed == "artifact":
                    config["artifact"]["digest"] = "sha256:" + "0" * 64
                elif changed in ("youki", "kernel", "initramfs", "version"):
                    config["artifact"][changed + "_sha256"] = "0" * 64
                elif changed == "release":
                    config["release_version"] = "0.0.1"
                elif changed == "name":
                    config["machine"]["name"] = "foreign"
                elif changed == "capabilities":
                    config["machine"]["requested_capabilities"] = {"capabilities": ["posix_exec"]}
                else:
                    config["backend"] = "foreign"
                raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
                self.write(self.configuration_path, raw, 0o400)
                self.receipt["configuration_digest"] = "sha256:" + sha(b"vz.machine-configuration.v1\0" + raw)
                owner = copy.deepcopy(self.owner_manifest)
                owner["configuration_digest"] = self.receipt["configuration_digest"]
                self.write(self.owner_path, json.dumps(owner).encode())
                self.proof()
                with self.assertRaises(driver.Rejected):
                    gate.authenticated_proof(self.h, self.environment, self.machine)
                self.assertFalse((self.h.evidence / "mch_test-runtime-binding.json").exists())


if __name__ == "__main__":
    unittest.main()
