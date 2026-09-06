"""Offline adversarial tests only; no Docker invocation or physical certification."""

import copy
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import docker_host_driver as driver


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        clients = {}
        for name in ("docker", "compose", "buildx"):
            path = self.root / ("docker" if name == "docker" else "docker-" + name)
            path.write_bytes(b"#!/bin/sh\nexit 99\n")
            path.chmod(0o700)
            clients[name] = {"path": str(path), "sha256": driver.sha256(path.read_bytes())}
        config = self.root / "config"
        config.mkdir(mode=0o700)
        (config / "config.json").write_text(json.dumps({"currentContext": "unused-decoy",
                                                       "cliPluginsExtraDirs": [str(self.root)]}))
        self.raw = {
            "schema_version": 1, "run_id": "fixture-run-12345678",
            "release_sha256": "a" * 64, "fixture_sha256": "b" * 64,
            "scope": {"project_id": "p", "environment_id": "e", "machine_id": "m",
                      "machine_incarnation": "i", "runtime_identity": "r",
                      "docker_context": "vz-owned-machine", "docker_endpoint": "unix://" + str(self.root / "machine.sock"),
                      "engine_id": "owned-engine"},
            "docker_config": str(config), "clients": clients,
            "images": {"base": {"reference": "registry.test/base@sha256:" + "c" * 64,
                                "id": "sha256:" + "d" * 64, "platform": "linux/arm64"},
                       "compose": {"reference": "sha256:" + "e" * 64,
                                   "id": "sha256:" + "e" * 64, "platform": "linux/arm64"}},
            "builder": {"name": "vz-owned-builder", "node": "vz-owned-builder0",
                        "container_id": "f" * 64, "image_id": "sha256:" + "0" * 64}}

    def bare_driver(self):
        item = driver.Driver.__new__(driver.Driver)
        item.inputs = driver.Inputs(copy.deepcopy(self.raw))
        item.temporary = self.root / "private-tmp"
        item.temporary.mkdir(exist_ok=True)
        item.output = self.root
        item.config_snapshot = item.validate_config()
        item.record = driver.Recorder(self.root, {"HOME": os.environ.get("HOME", ""), "PATH": "/usr/bin:/bin"}, [])
        item.observations = []
        item.projects = {}
        return item

    def test_valid_boundary_requires_no_process(self):
        with patch.object(driver, "execute") as process:
            inputs = driver.Inputs(self.raw)
        process.assert_not_called()
        self.assertTrue(inputs.owner.startswith("vz04-"))

    def builder_snapshot(self):
        builder = self.raw["builder"]
        return {"Id": builder["container_id"], "Image": builder["image_id"],
                "Name": "/buildx_buildkit_" + builder["node"], "RestartCount": 0,
                "Config": {"Env": ["PATH=/usr/bin:/bin", "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
                                   "BUILDKIT_SETUP_CGROUPV2_ROOT=1"]},
                "HostConfig": {"CgroupnsMode": "private", "Runtime": "youki", "Privileged": True, "Init": True},
                "State": {"Running": True, "Status": "running", "Pid": 737,
                          "StartedAt": "2026-09-06T05:50:57.314727448Z", "Paused": False,
                          "Restarting": False, "Dead": False}}

    def guarded_builder(self, item, snapshots):
        builder = self.raw["builder"]
        raw = (f"Name: {builder['name']}\nDriver: docker-container\n\nNodes:\nName: {builder['node']}\n"
               "Endpoint: vz-owned-machine\nStatus: running\n").encode()
        with patch.object(item, "guard"), patch.object(item, "command", return_value=subprocess.CompletedProcess([], 0, raw)), \
                patch.object(item, "json_command", side_effect=snapshots):
            for _ in snapshots:
                item.builder_guard()

    def test_builder_guards_require_private_setup_and_one_stable_process(self):
        original = self.builder_snapshot()
        self.assertEqual(original["State"]["StartedAt"], "2026-09-06T05:50:57.314727448Z")
        self.guarded_builder(self.bare_driver(), [[original], [copy.deepcopy(original)]])
        for key, value in (("Pid", 738), ("StartedAt", "2026-09-06T05:50:58.314727448Z")):
            with self.subTest(key=key):
                changed = copy.deepcopy(original); changed["State"][key] = value
                with self.assertRaisesRegex(ValueError, "process identity changed"):
                    self.guarded_builder(self.bare_driver(), [[original], [changed]])

    def test_builder_guard_rejects_cgroup_and_process_adversaries(self):
        original = self.builder_snapshot()
        cases = [("Config", "Env", original["Config"]["Env"][:-1]),
                 ("Config", "Env", original["Config"]["Env"] + ["BUILDKIT_SETUP_CGROUPV2_ROOT=1"]),
                 ("Config", "Env", original["Config"]["Env"] + ["BUILDKIT_SETUP_CGROUPV2_ROOT=0"]),
                 ("HostConfig", "CgroupnsMode", "host"), ("HostConfig", "CgroupnsMode", None),
                 ("HostConfig", "Runtime", "runc"), ("HostConfig", "Privileged", False),
                 ("HostConfig", "Init", False), ("HostConfig", "Init", 1),
                 ("State", "Pid", 0), ("State", "Pid", True), ("State", "Pid", "737"),
                 ("State", "StartedAt", "0001-01-01T00:00:00Z"),
                 ("State", "StartedAt", "2026-99-06T05:50:57Z"),
                 ("State", "Paused", True), ("State", "Restarting", True), ("State", "Dead", True)]
        for section, key, value in cases:
            with self.subTest(section=section, key=key, value=value):
                changed = copy.deepcopy(original); changed[section][key] = value
                with self.assertRaises(ValueError):
                    self.guarded_builder(self.bare_driver(), [[changed]])
        for snapshots in ([], [original, original]):
            with self.assertRaises(ValueError):
                self.guarded_builder(self.bare_driver(), [snapshots])
        for value in (1, True, None):
            changed = copy.deepcopy(original); changed["RestartCount"] = value
            with self.assertRaises(ValueError):
                self.guarded_builder(self.bare_driver(), [[changed]])

    def test_compose_only_omits_builder_but_build_all_and_suite_switch_reject(self):
        raw = copy.deepcopy(self.raw)
        del raw["builder"]
        driver.Inputs(raw, suite="compose")
        for suite in ("all", "build"):
            with self.subTest(suite=suite), self.assertRaises(driver.Rejected):
                driver.Inputs(raw, suite=suite)
        with self.assertRaises(driver.Rejected):
            driver.Inputs(raw | {"builder": None}, suite="compose")
        item = self.bare_driver()
        item.inputs = driver.Inputs(raw, suite="compose")
        with patch.object(item, "guard") as guard, self.assertRaises(driver.Rejected):
            item.run("all")
        guard.assert_not_called()

    def test_real_home_preserved_with_owned_temporary_directory(self):
        fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker"
        self.raw["fixture_sha256"] = driver.tree_digest(fixture)
        with patch.dict(os.environ, {"HOME": "/actual-user-home", "SSH_AUTH_SOCK": "forbidden"}), \
                patch.object(driver.sys, "platform", "darwin"), patch.object(driver.os, "uname") as uname:
            uname.return_value.machine = "arm64"
            item = driver.Driver(driver.Inputs(self.raw), fixture, self.root / "evidence")
        self.assertEqual(item.env["HOME"], "/actual-user-home")
        self.assertEqual(item.env["TMPDIR"], str(item.output / "private-tmp"))
        self.assertNotIn("SSH_AUTH_SOCK", item.env)
        self.assertFalse((item.output / "private-home").exists())

    def test_installed_exact_pinned_plugins_and_shadow_rejection(self):
        config = Path(self.raw["docker_config"])
        plugins = config / "cli-plugins"
        plugins.mkdir(mode=0o700)
        for name in ("compose", "buildx"):
            old = Path(self.raw["clients"][name]["path"])
            new = plugins / old.name
            new.write_bytes(old.read_bytes())
            new.chmod(0o700)
            self.raw["clients"][name]["path"] = str(new)
        (config / "config.json").write_text('{"currentContext":"unused-decoy"}')
        item = self.bare_driver()
        item.validate_config()
        unknown = plugins / "docker-unknown"
        unknown.write_bytes(b"unknown")
        with self.assertRaises(driver.Rejected):
            item.validate_config()
        unknown.unlink()
        (config / "config.json").write_text(json.dumps({"cliPluginsExtraDirs": [str(plugins)]}))
        with self.assertRaises(driver.Rejected):
            item.validate_config()

    def startup_proof(self):
        owner = {name: self.raw["scope"][name] for name in ("project_id", "environment_id", "machine_id")}
        incarnation = {"schema_version": 1, "machine_id": owner["machine_id"], "incarnation_id": "i", "generation": 1}
        inventory = {"owner": owner, "incarnation": incarnation, "youki_sha256": "1" * 64,
                     "scope": "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
                     "stdout": "vz-startup-runtime-inventory-v1\nyouki-sha256=" + "1" * 64 +
                     "\nyouki version: 0.7.0\nalternate-runtime-binaries=absent\n"}
        receipt = {"schema_version": 1, "state": "completed", "failure": None, "owner": owner,
                   "incarnation": incarnation, "context": self.raw["scope"]["docker_context"],
                   "client_sha256": self.raw["clients"]["docker"]["sha256"],
                   "resources": {"engine_id": "owned-engine", "runtime_inventory": inventory,
                                 "cleanup_scope": "disposable_probe_containers_compose_objects_and_images",
                                 "retained_buildkit_cache": True}}
        receipt_path = self.root / "receipt.json"
        receipt_path.write_text(json.dumps(receipt))
        after_path = self.root / "inventory.json"
        after_path.write_text(json.dumps({"schema_version": 1, "probe_receipt_sha256": driver.sha256(receipt_path.read_bytes()),
                                         "runtime_inventory": inventory}))
        self.raw["runtime_evidence"] = {"receipt_path": str(receipt_path), "receipt_sha256": driver.sha256(receipt_path.read_bytes()),
                                        "inventory_path": str(after_path), "inventory_sha256": driver.sha256(after_path.read_bytes()),
                                        "youki_sha256": "1" * 64}

    def test_runtime_proof_rehash_owner_incarnation_and_inventory_binding(self):
        self.startup_proof()
        inputs = driver.Inputs(self.raw)
        self.assertEqual(inputs.verify_runtime_evidence()["youki_sha256"], "1" * 64)
        for field in ("machine_id", "machine_incarnation", "engine_id", "docker_context"):
            raw = copy.deepcopy(self.raw)
            raw["scope"][field] = "foreign"
            with self.subTest(field=field), self.assertRaises(driver.Rejected):
                driver.Inputs(raw).verify_runtime_evidence()
        Path(self.raw["runtime_evidence"]["inventory_path"]).write_text('{}')
        with self.assertRaises(driver.Rejected):
            inputs.verify_runtime_evidence()

    def test_inert_runc_metadata_requires_verified_runtime_proof(self):
        self.startup_proof()
        item = self.bare_driver()
        context = [{"Name": "vz-owned-machine", "Endpoints": {"docker": {"Host": self.raw["scope"]["docker_endpoint"]}}}]
        info = {"ID": "owned-engine", "OSType": "linux", "Architecture": "aarch64", "DefaultRuntime": "youki",
                "Runtimes": {"youki": {"path": "/mnt/linux-bin/youki"}, "runc": {"path": "runc"}}}
        for proof, allowed in ((True, True), (False, False)):
            raw = copy.deepcopy(self.raw)
            if not proof:
                del raw["runtime_evidence"]
            item.inputs = driver.Inputs(raw)
            with patch.object(item, "json_command", side_effect=[context, info]), \
                    patch.object(driver.stat, "S_ISSOCK", return_value=True), patch.object(Path, "stat"):
                if allowed:
                    # Avoid mocking canonical-path resolution used by proof reads.
                    with patch.object(driver.Inputs, "verify_runtime_evidence", return_value={"verified": True}):
                        item.guard()
                else:
                    with self.assertRaises(driver.Rejected):
                        item.guard()

    def test_failed_mutation_keeps_uncertainty_until_durable_semantic_ack(self):
        item = self.bare_driver()
        with patch.object(driver, "execute", return_value=subprocess.CompletedProcess([], 37, b"out", b"err")):
            result = item.command(["exec", "exact-container", "fixture"], expected=37)
        self.assertTrue(item.record.receipts[0]["effects_uncertain"])
        self.assertTrue(item.cleanup())
        item.record.acknowledge_negative(result, "test exact expected output/exit proof")
        self.assertFalse(item.record.receipts[0]["effects_uncertain"])
        self.assertTrue((self.root / "command-00001.acknowledgement.json").is_file())

    def test_unexpected_failed_mutation_never_runs_cleanup(self):
        item = self.bare_driver()
        with patch.object(driver, "execute", return_value=subprocess.CompletedProcess([], 1, b"", b"request deadline")), \
                self.assertRaises(driver.Rejected):
            item.command(["compose", "up"])
        with patch.object(item, "compose") as cleanup:
            self.assertTrue(item.cleanup())
        cleanup.assert_not_called()

    def test_unlabelled_exact_resource_collision_rejected_before_admission(self):
        item = self.bare_driver()
        project = item.inputs.owner + "-compose"
        empty = {kind: [] for kind in ("container", "network", "volume")}
        with patch.object(item, "guard"), patch.object(item, "inspect_project", return_value=empty), \
                patch.object(item, "command", return_value=driver.Command(1, [], 0, (project + "_state\n").encode(), b"")), \
                self.assertRaises(driver.Rejected):
            item.new_project("compose")
        self.assertFalse(item.projects)

    def test_cleanup_rejects_mount_outside_captured_owned_inventory(self):
        item = self.bare_driver()
        project = "owned"
        item.projects[project] = {kind: set() for kind in ("container", "network", "volume")}
        inventory = {"container": [{"Id": "c", "Mounts": [{"Type": "volume", "Name": "foreign"}]}],
                     "volume": [], "network": []}
        with patch.object(item, "guard"), patch.object(item, "capture", return_value=inventory), \
                patch.object(item, "command", return_value=driver.Command(1, [], 0, b"", b"")), \
                patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    @staticmethod
    def unattached_inventory():
        # Shape observed in candidate3 command266/268: blocked dependency
        # containers exist, but Moby has not allocated their network endpoints.
        return {"volume": [], "network": [
            {"Name": "owned_frontend", "Id": "a" * 64, "Containers": {}},
            {"Name": "owned_backend", "Id": "b" * 64, "Containers": {}}],
            "container": [{"Id": "c" * 64, "Mounts": [],
                           "State": {"Status": "created", "Running": False, "Paused": False,
                                     "Restarting": False, "Pid": 0,
                                     "StartedAt": "0001-01-01T00:00:00Z",
                                     "FinishedAt": "0001-01-01T00:00:00Z"},
                           "HostConfig": {"NetworkMode": "owned_frontend"},
                           "NetworkSettings": {"Networks": {"owned_frontend": {
                               "NetworkID": "", "EndpointID": "", "Gateway": "", "IPAddress": "",
                               "MacAddress": "", "IPv6Gateway": "", "GlobalIPv6Address": "",
                               "IPPrefixLen": 0, "GlobalIPv6PrefixLen": 0}}}}]}

    def verify_network_inventory(self, inventory):
        item = self.bare_driver()
        def command(args, **kwargs):
            names = b"owned_frontend\nowned_backend\n" if args[0] == "network" else b""
            return driver.Command(1, [], 0, names, b"")
        with patch.object(item, "command", side_effect=command):
            item.verify_named_resources("owned", inventory)

    def test_never_started_owned_network_declaration_without_endpoint_is_safe(self):
        self.verify_network_inventory(self.unattached_inventory())

    def test_empty_network_identity_rejects_started_or_materialized_endpoint(self):
        changes = [("State", key, value) for key, value in (
            ("Status", "exited"), ("Running", True), ("Paused", True), ("Restarting", True),
            ("Pid", 1), ("Pid", False), ("StartedAt", "2026-09-05T00:00:00Z"),
            ("FinishedAt", "2026-09-05T00:00:00Z"))]
        changes += [("network", key, "nonempty") for key in (
            "EndpointID", "Gateway", "IPAddress", "MacAddress", "IPv6Gateway", "GlobalIPv6Address")]
        changes += [("network", key, 1) for key in ("IPPrefixLen", "GlobalIPv6PrefixLen")]
        changes += [("network", "IPPrefixLen", False), ("HostConfig", "NetworkMode", "foreign")]
        changes += [("network", "NetworkID", value) for value in (None, False, 0)]
        for section, key, value in changes:
            with self.subTest(section=section, key=key, value=value):
                inventory = self.unattached_inventory()
                container = inventory["container"][0]
                target = (container["NetworkSettings"]["Networks"]["owned_frontend"]
                          if section == "network" else container[section])
                target[key] = value
                with self.assertRaises(driver.Rejected):
                    self.verify_network_inventory(inventory)

    def test_network_empty_id_rejects_foreign_name_and_reverse_attachment(self):
        for case in ("foreign-name", "reverse-attachment", "duplicate-name", "duplicate-id",
                     "list-instead-of-containers-map", "missing-containers-map"):
            with self.subTest(case=case):
                inventory = self.unattached_inventory()
                if case == "foreign-name":
                    networks = inventory["container"][0]["NetworkSettings"]["Networks"]
                    networks["foreign"] = networks.pop("owned_frontend")
                elif case == "reverse-attachment":
                    inventory["network"][1]["Containers"]["c" * 64] = {"EndpointID": "foreign-endpoint"}
                elif case == "duplicate-name":
                    inventory["network"].append(copy.deepcopy(inventory["network"][0]))
                elif case == "duplicate-id":
                    inventory["network"][1]["Id"] = inventory["network"][0]["Id"]
                elif case == "list-instead-of-containers-map":
                    inventory["network"][1]["Containers"] = []
                else:
                    del inventory["network"][1]["Containers"]
                with self.assertRaises(driver.Rejected):
                    self.verify_network_inventory(inventory)

    def test_attached_network_requires_exact_name_to_id_binding(self):
        for network_id, allowed in (("a" * 64, True), ("b" * 64, False), ("f" * 64, False)):
            with self.subTest(network_id=network_id):
                inventory = self.unattached_inventory()
                inventory["container"][0]["NetworkSettings"]["Networks"]["owned_frontend"]["NetworkID"] = network_id
                if allowed:
                    self.verify_network_inventory(inventory)
                else:
                    with self.assertRaises(driver.Rejected):
                        self.verify_network_inventory(inventory)

    def test_cleanup_requires_exact_post_down_names_and_container_absence(self):
        for survivor in ("container", "volume", None):
            with self.subTest(survivor=survivor):
                item = self.bare_driver()
                item.projects = {"owned": {kind: set() for kind in ("container", "network", "volume")}}
                before = {"container": [{"Id": "c" * 64}], "volume": [], "network": []}
                empty = {kind: [] for kind in before}
                checks = []
                def command(args, **kwargs):
                    checks.append(args)
                    if args[:2] == ["container", "inspect"]:
                        return driver.Command(1, [], 0 if survivor == "container" else 1,
                                              b'[{}]' if survivor == "container" else b'[]\n',
                                              b'' if survivor == "container" else
                                              ("Error response from daemon: No such container: " + "c" * 64 + "\n").encode())
                    return driver.Command(1, [], 0, b"", b"")
                def names(project, inventory):
                    if not inventory["container"] and survivor == "volume":
                        raise driver.Rejected("exact unlabelled volume survived")
                with patch.object(item, "guard"), patch.object(item, "capture", return_value=before), \
                        patch.object(item, "inspect_project", return_value=empty), patch.object(item, "compose"), \
                        patch.object(item, "verify_named_resources", side_effect=names) as verified, \
                        patch.object(item, "command", side_effect=command):
                    errors = item.cleanup()
                self.assertEqual(bool(errors), survivor is not None)
                self.assertEqual(verified.call_count, 2)
                if survivor != "volume":
                    self.assertIn(["container", "inspect", "c" * 64], checks)

    def test_recreated_startup_sentinel_cannot_pass_host_volume_persistence(self):
        item = self.bare_driver()
        project = "owned"
        db = {"Id": "db", "Config": {"Labels": {"com.docker.compose.service": "db"}},
              "Mounts": [{"Destination": "/data", "Type": "volume", "RW": True, "Name": "owned_state"}]}
        inventory = {"container": [db], "volume": [{"Name": "owned_state"}], "network": []}
        marker = f"vz04|host-written|{item.inputs.owner}|{item.inputs.raw['run_id']}|persisted\n".encode()
        old_sentinel = f"vz04|db|{item.inputs.owner}|persisted\n".encode()
        with patch.object(item, "capture", return_value=inventory), patch.object(item, "compose"), \
                patch.object(item, "exec_container", side_effect=[driver.Command(1, [], 0, b"", b""),
                    driver.Command(2, [], 0, marker, b""), driver.Command(3, [], 0, old_sentinel, b"")]) as executed, \
                self.assertRaises(driver.Rejected):
            item.volume_persistence(project)
        self.assertIn("'xb'", executed.call_args_list[0].args[1][2])
        self.assertIn("host-persistence-", executed.call_args_list[-1].args[1][-1])

    def test_mutable_images_rejected_before_process(self):
        for reference in ("python:latest", "python:3.13", "sha256:" + "f" * 63,
                          "repo@sha256:" + "F" * 64, "--privileged"):
            with self.subTest(reference=reference), patch.object(driver, "execute") as process:
                raw = copy.deepcopy(self.raw)
                raw["images"]["base"]["reference"] = reference
                with self.assertRaises(driver.Rejected):
                    driver.Inputs(raw)
                process.assert_not_called()

    def test_wrong_platform_rejected(self):
        self.raw["images"]["base"]["platform"] = "linux/amd64"
        with self.assertRaises(driver.Rejected):
            driver.Inputs(self.raw)

    def test_global_contexts_and_remote_endpoints_rejected(self):
        for context in ("default", "desktop-linux", "orbstack", "--host"):
            raw = copy.deepcopy(self.raw)
            raw["scope"]["docker_context"] = context
            with self.subTest(context=context), self.assertRaises(driver.Rejected):
                driver.Inputs(raw)
        for endpoint in ("tcp://localhost:2375", "ssh://user@host", "unix:///var/run/docker.sock"):
            raw = copy.deepcopy(self.raw)
            raw["scope"]["docker_endpoint"] = endpoint
            with self.subTest(endpoint=endpoint), self.assertRaises(driver.Rejected):
                driver.Inputs(raw)

    def test_pin_and_symlink_rejected(self):
        raw = copy.deepcopy(self.raw)
        raw["clients"]["docker"]["sha256"] = "0" * 64
        with self.assertRaises(driver.Rejected):
            driver.Inputs(raw)
        target = self.root / "alias"
        target.symlink_to(self.root / "docker")
        raw["clients"]["docker"]["path"] = str(target)
        with self.assertRaises(driver.Rejected):
            driver.Inputs(raw)

    def test_client_config_rejects_credentials_and_helpers(self):
        item = self.bare_driver()
        path = Path(item.inputs.raw["docker_config"]) / "config.json"
        for key in ("credsStore", "credHelpers", "auths", "proxies"):
            path.write_text(json.dumps({"cliPluginsExtraDirs": [str(self.root)], key: "forbidden"}))
            with self.subTest(key=key), self.assertRaises(driver.Rejected):
                item.validate_config()

    def test_command_pins_explicit_config_context_and_records_exact_bytes(self):
        item = self.bare_driver()
        completed = subprocess.CompletedProcess([], 37, b"out\x00\n", b"err\n")
        with patch.object(driver, "execute", return_value=completed) as process:
            result = item.command(["compose", "version"], expected=37)
        self.assertEqual(result.stdout, b"out\x00\n")
        argv = process.call_args.args[0]
        self.assertEqual(argv[:5], ["docker", "--config", self.raw["docker_config"],
                                   "--context", "vz-owned-machine"])
        self.assertEqual(process.call_args.kwargs["executable"], self.raw["clients"]["docker"]["path"])
        env = process.call_args.kwargs["env"]
        self.assertFalse(any(key.startswith(("DOCKER_", "BUILDX_", "COMPOSE_", "SSH_")) for key in env))
        self.assertEqual((self.root / "command-00001.stdout").read_bytes(), b"out\x00\n")
        self.assertEqual(item.record.receipts[0]["exit_code"], 37)
        self.assertGreater(item.record.receipts[0]["elapsed_ns"], 0)
        self.assertEqual(item.record.receipts[0]["argv0"], "docker")
        self.assertEqual(item.record.receipts[0]["executable"], self.raw["clients"]["docker"]["path"])
        self.assertFalse(item.record.receipts[0]["effects_uncertain"])

    def test_real_isolated_multicall_process_receives_docker_argv0(self):
        # A compiled local fixture actually sees kernel-provided argv[0]. A
        # shebang script cannot establish this because its interpreter rewrites
        # argv. This invokes neither the user's Docker binary nor any daemon.
        (self.root / "bin").mkdir()
        binary = self.root / "bin/docker-tools"
        source = b'''#include <stdio.h>
#include <string.h>
int main(int argc, char **argv) {
    (void)argc;
    puts(argv[0]);
    return strcmp(argv[0], "docker") == 0 ? 0 : 73;
}
'''
        compiled = subprocess.run(["/usr/bin/clang", "-x", "c", "-", "-o", str(binary)], input=source,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
        self.assertEqual(compiled.returncode, 0, compiled.stderr)
        self.raw["clients"]["docker"] = {"path": str(binary), "sha256": driver.sha256(binary.read_bytes())}
        item = self.bare_driver()
        result = item.command(["version"])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"docker\n")
        receipt = item.record.receipts[0]
        self.assertEqual(receipt["executable"], str(binary))
        self.assertEqual(receipt["argv"][0], "docker")
        wrong = driver.execute([str(binary)], executable=str(binary), timeout=5, check=False,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(wrong.returncode, 73)

    def test_inflight_uncertainty_is_durable_before_dispatch(self):
        item = self.bare_driver()

        def inspect_intent(*args, **kwargs):
            intent = json.loads((self.root / "command-00001.intent.json").read_text())
            self.assertEqual(intent["host_outcome"], "inflight")
            self.assertTrue(intent["effects_uncertain"])
            self.assertTrue(item.record.receipts[0]["effects_uncertain"])
            self.assertTrue(item.cleanup())
            return subprocess.CompletedProcess([], 0, b"", b"")

        with patch.object(driver, "execute", side_effect=inspect_intent):
            item.command(["version"])
        self.assertFalse(item.record.receipts[0]["effects_uncertain"])

    def test_keyboard_interrupt_retained_even_after_owned_group_is_reaped(self):
        item = self.bare_driver()
        interrupted = KeyboardInterrupt()
        interrupted.stdout, interrupted.stderr = b"dispatched-before-interrupt", b"partial"
        with patch.object(subprocess, "Popen") as popen, patch.object(os, "killpg") as kill, \
                patch.object(driver, "collect_output", side_effect=interrupted):
            process = popen.return_value
            process.pid = 45678
            process.returncode = None
            with self.assertRaises(KeyboardInterrupt):
                item.command(["compose", "up"])
        kill.assert_called_once_with(45678, driver.signal.SIGKILL)
        self.assertEqual(process.wait.call_count, 1)
        receipt = json.loads((self.root / "command-00001.json").read_text())
        self.assertTrue(receipt["interrupted"])
        self.assertTrue(receipt["effects_uncertain"])
        self.assertEqual(receipt["host_outcome"], "interrupted")
        self.assertEqual((self.root / "command-00001.stdout").read_bytes(), b"dispatched-before-interrupt")
        with patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    def test_unknown_spawn_observation_withholds_destructive_cleanup(self):
        item = self.bare_driver()
        with patch.object(driver, "execute", side_effect=OSError("dispatch state unknown")), self.assertRaises(OSError):
            item.command(["compose", "up"])
        receipt = json.loads((self.root / "command-00001.json").read_text())
        self.assertEqual(receipt["host_outcome"], "unknown")
        self.assertTrue(receipt["effects_uncertain"])
        with patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    def test_interrupted_terminal_receipt_write_leaves_inflight_uncertainty(self):
        item = self.bare_driver()
        persist = item.record.persist

        def interrupt_terminal(path, value, **kwargs):
            if not path.name.endswith(".intent.json"):
                raise KeyboardInterrupt()
            persist(path, value, **kwargs)

        with patch.object(driver, "execute", return_value=subprocess.CompletedProcess([], 0, b"", b"")), \
                patch.object(item.record, "persist", side_effect=interrupt_terminal), self.assertRaises(KeyboardInterrupt):
            item.command(["compose", "up"])
        self.assertTrue(item.record.receipts[0]["effects_uncertain"])
        self.assertEqual(item.record.receipts[0]["host_outcome"], "inflight")
        self.assertTrue((self.root / "command-00001.intent.json").is_file())
        with patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    def test_cleanup_uncertainty_stops_all_remaining_projects(self):
        item = self.bare_driver()
        item.projects = {"first": {}, "second": {}}

        def failed_observation():
            item.record.receipts.append({"effects_uncertain": True, "timed_out": True})
            raise driver.Rejected("cleanup routing observation timed out")

        with patch.object(item, "guard", side_effect=failed_observation) as guard, \
                patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        self.assertEqual(guard.call_count, 1)
        destructive.assert_not_called()

    def test_noisy_synthetic_process_is_bounded_reaped_and_never_a_success(self):
        # Invoke only the test interpreter, never Docker. Each case writes
        # forever to one chosen stream, exercising independent pipe bounds.
        for stream in (1, 2):
            with self.subTest(stream=stream):
                root = self.root / f"noise-{stream}"
                root.mkdir()
                record = driver.Recorder(root, {"PATH": "/usr/bin:/bin"}, [], max_stream_bytes=1024)
                code = f"import os; os.write({stream}, (str(os.getpid()) + '\\n').encode()); exec('while True: os.write({stream}, b\\\"x\\\" * 4096)')"
                with self.assertRaises(driver.OutputLimitExceeded):
                    record.run([sys.executable, "-c", code], executable=sys.executable, timeout=5)
                receipt = record.receipts[0]
                self.assertTrue(receipt["output_limit_exceeded"])
                self.assertTrue(receipt["effects_uncertain"])
                self.assertFalse(receipt["capture_complete"])
                self.assertFalse(receipt["raw_streams_retained"])
                self.assertIsNone(receipt["raw_stdout_sha256"])
                self.assertIsNone(receipt["raw_stderr_sha256"])
                name = "stdout" if stream == 1 else "stderr"
                raw = (root / ("command-00001." + name)).read_bytes()
                self.assertEqual(len(raw), 1024)
                self.assertEqual(receipt["observed_bytes"][name], 1025)
                self.assertEqual(receipt["retained_observed_" + name + "_sha256"], driver.sha256(raw))
                pid = int(raw.split(b"\n", 1)[0])
                with self.assertRaises(ChildProcessError):
                    os.waitpid(pid, os.WNOHANG)
                item = self.bare_driver()
                item.record = record
                with patch.object(item, "compose") as cleanup:
                    self.assertTrue(item.cleanup())
                cleanup.assert_not_called()

    def test_real_dual_stream_at_exact_limit_completes_without_truncation(self):
        code = "import os; os.write(1, b'a' * 1024); os.write(2, b'b' * 1024)"
        result = driver.execute([sys.executable, "-c", code], executable=sys.executable,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=False,
                                max_stream_bytes=1024)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"a" * 1024)
        self.assertEqual(result.stderr, b"b" * 1024)

    def test_real_partial_output_timeout_preserves_only_observed_prefix(self):
        root = self.root / "partial-timeout"
        root.mkdir()
        record = driver.Recorder(root, {"PATH": "/usr/bin:/bin"}, [], max_stream_bytes=1024)
        collect = driver.collect_output
        def ready_capture(process, timeout, limit):
            # Bootstrap scheduling is not the timeout behavior under test.
            # Observe readiness without consuming either stream, then start
            # the real bounded collector with stdout already in its pipe.
            with driver.selectors.DefaultSelector() as selector:
                selector.register(process.stderr, driver.selectors.EVENT_READ)
                self.assertTrue(selector.select(10), "synthetic child did not become ready")
            return collect(process, timeout, limit)
        with patch.object(driver, "collect_output", side_effect=ready_capture):
            result = record.run([sys.executable, "-c",
                                 "import os,time;os.write(1,b'partial');os.write(2,b'ready\\n');time.sleep(10)"],
                                executable=sys.executable, timeout=0.2)
        self.assertTrue(result.timed_out)
        self.assertEqual(result.stdout, b"partial")
        self.assertIsNone(record.receipts[0]["raw_stdout_sha256"])
        self.assertEqual(record.receipts[0]["retained_observed_stdout_sha256"], driver.sha256(b"partial"))

    def test_changed_executable_rejected_before_command(self):
        item = self.bare_driver()
        (self.root / "docker-compose").write_bytes(b"modified\n")
        with patch.object(driver, "execute") as process, self.assertRaises(driver.Rejected):
            item.command(["compose", "version"])
        process.assert_not_called()

    def test_timeout_is_recorded_once_not_expected_negative_success(self):
        item = self.bare_driver()
        with patch.object(driver, "execute", side_effect=subprocess.TimeoutExpired([], 1, output=b"partial")) as process:
            with self.assertRaises(driver.Rejected):
                item.command(["version"])
        self.assertEqual(process.call_count, 1)
        self.assertTrue(item.record.receipts[0]["timed_out"])
        self.assertEqual((self.root / "command-00001.stdout").read_bytes(), b"partial")

    def test_secret_leak_is_failure_and_never_published(self):
        item = self.bare_driver()
        item.record.canaries = [b"PRIVATE-CANARY"]
        with patch.object(driver, "execute", return_value=subprocess.CompletedProcess([], 0, b"PRIVATE-CANARY", b"")):
            with self.assertRaises(driver.Rejected):
                item.command(["version"])
        for path in self.root.glob("command-*"):
            self.assertNotIn(b"PRIVATE-CANARY", path.read_bytes())
        self.assertTrue(item.record.receipts[0]["secret_leak_detected"])
        self.assertFalse(item.record.receipts[0]["raw_streams_retained"])

    def test_escaped_private_arguments_and_environments_rejected_before_intent(self):
        for secret in ('private\ncanary', 'private"canary', 'private\\canary'):
            for location in ('argv', 'environment', 'base_environment'):
                with self.subTest(secret=secret, location=location):
                    record = driver.Recorder(self.root, {"PRIVATE": secret} if location == 'base_environment' else {},
                                             [secret.encode()])
                    argv = ['fake', secret] if location == 'argv' else ['fake']
                    env = {"PRIVATE": secret} if location == 'environment' else {}
                    with patch.object(driver, 'execute') as execute, self.assertRaises(driver.Rejected):
                        record.run(argv, executable='/pinned/fake', extra_env=env)
                    execute.assert_not_called()
                    self.assertEqual(record.count, 0)
                    self.assertEqual(record.receipts, [])
                    self.assertFalse(list(self.root.glob('command-*')))

    def test_unicode_escaped_json_output_is_withheld(self):
        item = self.bare_driver()
        item.record.canaries = [b'PRIVATE-CANARY']
        raw = b'{"message":"\\u0050RIVATE-CANARY"}\n'
        with patch.object(driver, 'execute', return_value=subprocess.CompletedProcess([], 0, b'', raw)), \
                self.assertRaises(driver.Rejected):
            item.command(['version'])
        for path in self.root.glob('command-*'):
            self.assertNotIn(raw.strip(), path.read_bytes())
        self.assertTrue(item.record.receipts[0]['secret_leak_detected'])
        self.assertFalse(item.record.receipts[0]['raw_streams_retained'])

    def test_wrong_context_rejected_before_engine_contact(self):
        item = self.bare_driver()
        with patch.object(item, "json_command", return_value=[{
            "Name": "vz-owned-machine", "Endpoints": {"docker": {"Host": "unix:///foreign.sock"}}}]) as command:
            with self.assertRaises(driver.Rejected):
                item.guard()
        self.assertEqual(command.call_count, 1)

    def test_wrong_engine_and_runtime_rejected(self):
        item = self.bare_driver()
        context = [{"Name": "vz-owned-machine", "Endpoints": {"docker": {"Host": self.raw["scope"]["docker_endpoint"]}}}]
        info = {"ID": "owned-engine", "OSType": "linux", "Architecture": "aarch64",
                "DefaultRuntime": "youki", "Runtimes": {"youki": {}}}
        for changed in ({"ID": "foreign-engine"}, {"DefaultRuntime": "runc"},
                        {"Runtimes": {"youki": {}, "runc": {}}}):
            with self.subTest(changed=changed), patch.object(item, "json_command", side_effect=[context, info | changed]), \
                    patch.object(driver.stat, "S_ISSOCK", return_value=True), \
                    patch.object(Path, "stat"), self.assertRaises(driver.Rejected):
                item.guard()

    def test_foreign_owner_resource_cannot_be_captured_or_deleted(self):
        item = self.bare_driver()
        foreign = {"Id": "f" * 64, "Config": {"Labels": {"com.docker.compose.project": "owned-project"}}}
        with patch.object(item, "command", return_value=driver.Command(1, [], 0, b"container-id\n", b"")), \
                patch.object(item, "json_command", return_value=[foreign]), self.assertRaises(driver.Rejected):
            item.inspect_project("owned-project")

    def test_empty_existing_project_claim_rejected(self):
        item = self.bare_driver()
        with patch.object(item, "guard"), patch.object(item, "inspect_project", return_value={"container": [{}]}), \
                self.assertRaises(driver.Rejected):
            item.new_project("compose")
        self.assertEqual(item.projects, {})

    def test_observation_cannot_pass_without_assertions(self):
        item = self.bare_driver()
        with self.assertRaises(driver.Rejected):
            item.observe("bad", ["docker.compose.up"], lambda: [])
        self.assertEqual(item.observations[0]["outcome"], "failed")


class AssertionTests(unittest.TestCase):
    def test_denied_group_signal_retains_prefix_without_broad_fallback(self):
        original = driver.OutputLimitExceeded("bounded output")
        original.stdout, original.stderr, original.observed_bytes = b"prefix", b"", {"stdout": 7, "stderr": 0}
        with patch.object(subprocess, "Popen") as popen, patch.object(os, "killpg", side_effect=PermissionError("denied")) as kill, \
                patch.object(driver, "collect_output", side_effect=original):
            process = popen.return_value
            process.pid, process.returncode = 12345, None
            with self.assertRaises(PermissionError) as raised:
                driver.execute(["owned"], timeout=2, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            kill.assert_called_once_with(12345, driver.signal.SIGKILL)
            process.wait.assert_not_called()
            process.__exit__.assert_not_called()
            self.assertEqual(raised.exception.stdout, b"prefix")

    def test_failed_sigkill_reap_is_bounded_without_context_manager_wait(self):
        original = driver.OutputLimitExceeded("bounded output")
        original.stdout, original.stderr = b"prefix", b""
        with patch.object(subprocess, "Popen") as popen, patch.object(os, "killpg"), \
                patch.object(driver, "collect_output", side_effect=original):
            process = popen.return_value
            process.pid, process.returncode = 12345, None
            process.wait.side_effect = subprocess.TimeoutExpired(["owned"], 5)
            with self.assertRaises(subprocess.TimeoutExpired) as raised:
                driver.execute(["owned"], timeout=2, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            process.wait.assert_called_once_with(timeout=5)
            process.__exit__.assert_not_called()
            self.assertEqual(raised.exception.stdout, b"prefix")

    def test_blocked_events_require_positive_complete_actor_history(self):
        ids = {role: role for role in ("db", "api", "worker", "isolated")}
        events = [self.event(role, "create", 1) for role in ids]
        events += [self.event("db", "start", 2), self.event("db", "health_status: unhealthy", 3)]
        driver.assert_blocked_events(events, ids, "owned")
        for bad in ([], events[1:], events + [self.event("worker", "start", 4)],
                    events + [self.event("foreign", "create", 5)], events[:-1]):
            with self.subTest(events=bad), self.assertRaises(driver.Rejected):
                driver.assert_blocked_events(bad, ids, "owned")

    def test_transport_http_errors_unclassified_and_ip_dns_are_not_denials(self):
        url = "http://172.18.0.2:8080/health"
        row = {"schema_version": 1, "url": url, "outcome": "timeout", "status": None,
               "errno": None, "exception": "TimeoutError"}
        def command(value):
            return driver.Command(1, [], 0, json.dumps(value).encode(), b"")
        driver.assert_transport_denied(command(row), url, dns_name=False)
        for bad in (row | {"outcome": "http_response", "status": 503}, row | {"outcome": "probe_error"},
                    row | {"outcome": "dns_failure"}, row | {"url": "http://foreign:8080/health"}):
            with self.subTest(row=bad), self.assertRaises(driver.Rejected):
                driver.assert_transport_denied(command(bad), url, dns_name=False)

    def test_owned_process_group_is_killed_on_timeout(self):
        with patch.object(subprocess, "Popen") as popen, patch.object(os, "killpg") as kill, \
                patch.object(driver, "collect_output", side_effect=subprocess.TimeoutExpired(["owned"], 2)):
            process = popen.return_value
            process.pid = 12345
            process.returncode = None
            with self.assertRaises(subprocess.TimeoutExpired):
                driver.execute(["owned"], timeout=2, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            kill.assert_called_once_with(12345, driver.signal.SIGKILL)
            self.assertEqual(process.wait.call_count, 1)

    def test_base64_split_buildkit_logs_are_scanned_for_canaries(self):
        raw = b"\n".join(json.dumps({"data": base64.b64encode(part).decode()}).encode()
                         for part in (b"PRIVATE-", b"CANARY"))
        self.assertTrue(driver.contains_canary((raw,), [b"PRIVATE-CANARY"]))

    def test_invalid_log_cannot_hide_later_encoded_canary(self):
        raw = json.dumps({"logs": [{"data": "not base64"},
                                   {"data": base64.b64encode(b"PRIVATE-CANARY").decode()}]}).encode()
        self.assertTrue(driver.contains_canary((raw,), [b"PRIVATE-CANARY"]))
        self.assertFalse(driver.contains_canary((b"ordinary output",), [b"PRIVATE-CANARY"]))

    def test_batched_buildkit_logs_are_scanned_across_status_frames(self):
        raw = b"\n".join(json.dumps({"logs": [{"data": base64.b64encode(part).decode()}]}).encode()
                         for part in (b"PRIVATE-", b"CANARY"))
        self.assertTrue(driver.contains_canary((raw,), [b"PRIVATE-CANARY"]))

    def test_decoded_json_strings_keys_arrays_and_duplicate_keys_are_scanned(self):
        rows = [b'{"message":"\\u0050RIVATE-CANARY"}', b'["\\u0050RIVATE-CANARY"]',
                b'{"\\u0050RIVATE-CANARY":false}',
                b'{"message":"\\u0050RIVATE-CANARY","message":"ordinary"}',
                b'{"message":[["\\u0050RIVATE-CANARY"]],"message":"ordinary"}']
        for raw in rows:
            with self.subTest(raw=raw):
                self.assertTrue(driver.contains_canary((raw,), [b'PRIVATE-CANARY']))
        self.assertFalse(driver.contains_canary((b'{"message":"ordinary"}',), [b'PRIVATE-CANARY']))

    def test_decoded_buildkit_json_metadata_is_scanned(self):
        raw = json.dumps({'logs': [{'data': base64.b64encode(
            b'{"message":"\\u0050RIVATE-CANARY"}\n').decode()}]}).encode()
        self.assertTrue(driver.contains_canary((raw,), [b'PRIVATE-CANARY']))

    def test_pretty_json_documents_and_duplicate_keys_are_scanned(self):
        secret = b'PRIVATE-CANARY'
        rows = [json.dumps({'message': secret.decode()}, indent=2).encode().replace(b'PRIVATE', b'\\u0050RIVATE'),
                json.dumps([{'message': secret.decode()}], indent=2).encode().replace(b'PRIVATE', b'\\u0050RIVATE'),
                b'{\n  "message": "\\u0050RIVATE-CANARY",\n  "message": "ordinary"\n}\n']
        for raw in rows:
            with self.subTest(raw=raw):
                self.assertTrue(driver.contains_canary((raw,), [secret]))
        self.assertFalse(driver.contains_canary((json.dumps({'message': ['ordinary']}, indent=2).encode(),), [secret]))

    def test_nested_base64_and_excessive_json_depth_fail_closed(self):
        raw = b'ordinary'
        for _ in range(20):
            raw = json.dumps({'data': base64.b64encode(raw).decode()}).encode()
        self.assertTrue(driver.contains_canary((raw,), [b'PRIVATE-CANARY']))
        self.assertTrue(driver.contains_canary((b'[' * 2000 + b'0' + b']' * 2000,), [b'PRIVATE-CANARY']))
        self.assertTrue(driver._contains_canary((b'ordinary',), [b'PRIVATE-CANARY'], 0, [1]))

    def event(self, actor, action, timestamp):
        return {"Type": "container", "Actor": {"ID": actor, "Attributes": {"com.docker.compose.project": "owned"}},
                "Action": action, "timeNano": timestamp}

    def healthy_events(self):
        return [self.event("db", "health_status: healthy", 1), self.event("api", "start", 2),
                self.event("api", "health_status: healthy", 3), self.event("worker", "start", 4)]

    def test_event_order_is_engine_timestamp_not_arrival_order(self):
        driver.assert_health_order(list(reversed(self.healthy_events())), {"db": "db", "api": "api", "worker": "worker"}, "owned")

    def test_missing_foreign_repeated_and_wrong_order_events_rejected(self):
        for events in (self.healthy_events()[:-1], self.healthy_events() + [self.event("foreign", "start", 10)],
                       self.healthy_events() + [self.event("api", "start", 20)],
                       [self.event("db", "health_status: healthy", 5), *self.healthy_events()[1:]]):
            with self.subTest(events=events), self.assertRaises(driver.Rejected):
                driver.assert_health_order(events, {"db": "db", "api": "api", "worker": "worker"}, "owned")

    def test_cache_requires_terminal_exact_vertex_and_observed_cached_boolean(self):
        vertex = {"digest": "sha256:" + "a" * 64, "name": "[build 3/3] RUN --network=none python3 /fixture/tools.py payload",
                  "completed": "2026-09-05T00:00:00Z", "cached": True}
        raw = json.dumps({"vertexes": [vertex]}).encode()
        driver.assert_payload_vertex(raw, cached=True)
        with self.assertRaises(driver.Rejected):
            driver.assert_payload_vertex(raw, cached=False)
        for bad in (b"", b"CACHED", json.dumps({"vertexes": [vertex | {"completed": None}]}).encode(),
                    raw + b"\n" + raw, json.dumps(vertex).encode(),
                    json.dumps({"vertexes": [vertex | {"cached": "true"}]}).encode()):
            with self.subTest(bad=bad), self.assertRaises((driver.Rejected, ValueError)):
                driver.assert_payload_vertex(bad, cached=True)

    def test_batched_payload_omitted_false_cache_and_log_frames(self):
        vertex = {"digest": "sha256:" + "a" * 64,
                  "name": "[build 3/3] RUN --network=none python3 /fixture/tools.py payload",
                  "completed": "2026-09-05T00:00:00.123456789Z"}
        raw = json.dumps({"logs": [{"data": "aGk="}]}).encode() + b"\n" + json.dumps({"vertexes": [vertex]}).encode()
        self.assertEqual(driver.assert_payload_vertex(raw, cached=False), vertex["digest"])

    def secret_failure(self, *, excerpt=True):
        source = (Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker/build/Dockerfile.secret").read_bytes()
        vertex = {"digest": "sha256:" + "a" * 64, "name": "[build 2/3] " + source.decode().splitlines()[5],
                  "completed": "2026-09-05T00:00:00Z", "error": "secret fixture: not found"}
        lines = [json.dumps({"vertexes": [vertex]})]
        if excerpt:
            lines += ["Dockerfile.secret:6", "--------------------",
                      "   4 |     ARG FIXTURE_SECRET_SHA256",
                      "   5 |     COPY tools.py /fixture/tools.py",
                      "   6 | >>> " + source.decode().splitlines()[5],
                      "   7 |     RUN --network=none test ! -e /run/secrets/fixture",
                      "   8 |     FROM scratch AS output", "--------------------"]
        lines += ["ERROR: failed to build: failed to solve: secret fixture: not found"]
        return source, vertex, ("\n".join(lines) + "\n").encode()

    def test_required_secret_accepts_pinned_rawjson_with_exact_cli_trailer(self):
        for excerpt in (True, False):
            source, vertex, raw = self.secret_failure(excerpt=excerpt)
            self.assertEqual(driver.assert_required_secret_failure(raw, source), vertex["digest"])

    def test_required_secret_rejects_unproven_errors_and_malformed_trailers(self):
        source, vertex, raw = self.secret_failure()
        mutations = [b"", b"x" * (driver.MAX_STREAM_BYTES + 1), raw + b"unexpected\n",
                     raw.replace(b"Dockerfile.secret:6", b"Dockerfile.secret:5"),
                     raw.replace(b"FROM scratch AS output", b"FROM unexpected"),
                     raw.replace(b"ERROR: failed to build: failed to solve:", b"ERROR: unavailable:"),
                     raw[:raw.index(b"ERROR:")], raw[raw.index(b"Dockerfile.secret:"):],
                     b'{malformed}\n' + raw, b'[]\n' + raw,
                     b'{"vertexes":[],"vertexes":[]}\n' + raw,
                     b'{"vertexes":null}\n' + raw, b'{"logs":[null]}\n' + raw,
                     b'{"unknown":[]}\n' + raw, b'{"vertexes":[NaN]}\n' + raw,
                     raw + json.dumps({"vertexes": [vertex]}).encode() + b"\n"]
        for change in ({"error": "secret fixture: permission denied"}, {"error": "secret other: not found"},
                       {"error": "connection refused"}, {"cached": True}, {"cached": "false"},
                       {"completed": None}, {"completed": "yes"}, {"digest": "not-a-digest"},
                       {"name": "[build 2/3] RUN echo secret fixture not found"}, {"error": ""}):
            mutations.append(json.dumps({"vertexes": [vertex | change]}).encode() + b"\n" + raw.split(b"\n", 1)[1])
        mutations.append(json.dumps({"vertexes": [vertex, vertex]}).encode() + b"\n" + raw.split(b"\n", 1)[1])
        mutations.append(json.dumps({"vertexes": [vertex | {"name": "[internal] load metadata", "error": "registry unavailable"}]}).encode() + b"\n" + raw)
        for bad in mutations:
            with self.subTest(bad=bad[:180]), self.assertRaises(ValueError):
                driver.assert_required_secret_failure(bad, source)

    def test_required_secret_rejects_unpinned_outer_wrapper_and_extra_footer(self):
        for excerpt in (True, False):
            source, _, raw = self.secret_failure(excerpt=excerpt)
            footer = b"ERROR: failed to build: failed to solve: secret fixture: not found\n"
            self.assertTrue(raw.endswith(footer))
            alternatives = (
                b"ERROR: failed to solve: secret fixture: not found\n",
                b"ERROR: failed to build: failed to build: failed to solve: secret fixture: not found\n",
                b"ERROR: unrelated failure: failed to solve: secret fixture: not found\n",
                footer + footer,
                footer + b"unexpected trailing diagnostic\n",
            )
            for replacement in alternatives:
                with self.subTest(excerpt=excerpt, replacement=replacement), self.assertRaises(ValueError):
                    driver.assert_required_secret_failure(raw[:-len(footer)] + replacement, source)

    def test_builder_single_exact_node(self):
        builder = {"name": "builder", "node": "builder0"}
        raw = b"Name: builder\nDriver: docker-container\n\nNodes:\nName: builder0\nEndpoint: machine\nStatus: running\n"
        driver.assert_builder_inspect(raw, builder, "machine")
        for bad in (raw.replace(b"machine", b"foreign"), raw + b"Name: another\n", raw + b"Error: failed\n",
                    raw.replace(b"running", b"stopped")):
            with self.subTest(bad=bad), self.assertRaises(driver.Rejected):
                driver.assert_builder_inspect(bad, builder, "machine")

    def test_exports_reject_extra_files_symlinks_and_wrong_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "payload.txt"
            path.write_bytes(b"exact")
            driver.assert_export(root, "payload.txt", b"exact")
            with self.assertRaises(driver.Rejected):
                driver.assert_export(root, "payload.txt", b"wrong")
            (root / "intermediate-canary").write_bytes(b"leaked")
            with self.assertRaises(driver.Rejected):
                driver.assert_export(root, "payload.txt", b"exact")

    def test_result_cannot_claim_release_pass_or_no_work_success(self):
        result = {"schema_version": 1, "compatibility_certified": False, "release_scenarios_passed": [],
                  "test_case_retries": 0, "observations": [], "outcome": "fixture_assertions_passed",
                  "failure": None, "cleanup_errors": [], "suite": "compose", "command_count": 0}
        with self.assertRaises(driver.Rejected):
            driver.validate_result(result)
        result["outcome"] = "failed"
        driver.validate_result(result)
        for key, value in (("compatibility_certified", True), ("release_scenarios_passed", ["docker.compose.up"]),
                           ("test_case_retries", 1)):
            with self.subTest(key=key), self.assertRaises(driver.Rejected):
                driver.validate_result(result | {key: value})

    def test_result_rejects_duplicate_recipe_and_unexecuted_success(self):
        observations = [{"recipe": recipe, "outcome": "fixture_assertions_passed", "first_command": index + 1,
                         "last_command": index + 1, "assertions": ["offline fixture assertion"]}
                        for index, recipe in enumerate(driver.COMPOSE_RECIPES)]
        observation = observations[0]
        result = {"schema_version": 1, "compatibility_certified": False, "release_scenarios_passed": [],
                  "test_case_retries": 0, "observations": observations, "outcome": "fixture_assertions_passed",
                  "failure": None, "cleanup_errors": [], "suite": "compose", "command_count": len(observations)}
        driver.validate_result(result)
        with self.assertRaises(driver.Rejected):
            driver.validate_result(result | {"observations": [observation, observation]})
        with self.assertRaises(driver.Rejected):
            driver.validate_result(result | {"observations": [observation | {"last_command": 0}]})

    def test_timeout_never_dispatches_cleanup(self):
        item = driver.Driver.__new__(driver.Driver)
        item.record = type("Record", (), {"receipts": [{"timed_out": True}]})()
        item.projects = {"owned": {}}
        with patch.object(item, "compose") as compose:
            errors = item.cleanup()
        self.assertTrue(errors)
        compose.assert_not_called()


class PayloadGraphTests(unittest.TestCase):
    base = "python@sha256:" + "f" * 64
    fixture_digest = "e" * 64

    def graph(self, *, cached=False, solve=0):
        names = {
            "base": "[build 1/3] FROM docker.io/library/" + self.base,
            "context": "[internal] load build context",
            "copy": "[build 2/3] COPY tools.py input.txt intermediate-canary.txt /fixture/",
            "run": "[build 3/3] RUN --network=none python3 /fixture/tools.py payload",
            "output": "[output 1/1] COPY --from=build /out/payload.txt /payload.txt",
        }
        edges = {"base": [], "context": [], "copy": ["base", "context"], "run": ["copy"], "output": ["run"]}
        ids = {role: "sha256:" + driver.sha256((role + str(solve if role != "base" else 0)).encode()) for role in names}
        stamp = lambda second: f"2026-09-06T06:00:{solve * 10 + second:02d}.123456789Z"
        periods = {"base": (1, 2), "context": (1, 2), "copy": (2, 3), "run": (3, 4), "output": (4, 5)}
        vertices = [{"digest": ids[role], "name": name, "inputs": [ids[p] for p in edges[role]],
                     "started": stamp(periods[role][0]), "completed": stamp(periods[role][1]), "cached": cached}
                    for role, name in names.items()]
        logs = [] if cached else [{"vertex": ids["run"], "stream": 1, "timestamp": stamp(3),
                                  "data": base64.b64encode(b"vz04-payload-step-executed\n").decode()}]
        return {"vertexes": vertices, "logs": logs}

    def encoded(self, batch):
        return json.dumps(batch).encode() + b"\n"

    def proof(self, batch, cached=False):
        return driver.assert_payload_graph(self.encoded(batch), self.base, cached=cached)

    def command(self, batch, *, solve=0, variant="alpha"):
        dest = Path(f"/private/owned/export-{solve}")
        argv = ["docker", "--config", "/private/owned/docker", "--context", "owned-machine", "buildx", "build",
                "--builder", "owned-builder", "--platform", "linux/arm64", "--file", "/private/fixture/Dockerfile",
                "--output", "type=local,dest=" + str(dest), "--build-arg", "FIXTURE_BASE=" + self.base,
                "--build-arg", "FIXTURE_RUN=owned-run", "--build-arg", "FIXTURE_VARIANT=" + variant, "/private/fixture"]
        result = driver.Command(solve + 1, argv, 0, b"", self.encoded(batch))
        result.build_binding = driver.bind_build_command(argv, list(argv), dest, self.fixture_digest, b"exact Dockerfile")
        result.build_engine_ns = driver.build_timestamp(f"2026-09-06T06:00:{solve * 10:02d}Z")
        return result

    def test_changed_solve_ids_preserve_only_proven_operation_identity(self):
        first, second = self.graph(), self.graph(cached=True, solve=1)
        a, b = self.proof(first), self.proof(second, True)
        self.assertNotEqual(a["vertices"]["run"], b["vertices"]["run"])
        driver.assert_payload_pair(self.command(first), a, self.command(second, solve=1), b)
        beta = self.graph(solve=2)
        driver.assert_payload_pair(self.command(first), a, self.command(beta, solve=2, variant="beta"), self.proof(beta), variant=True)

    def test_source_multi_phase_progress_is_accepted_without_identity_drift(self):
        batch = self.graph()
        base = copy.deepcopy(batch["vertexes"][0]); context = copy.deepcopy(batch["vertexes"][1])
        batch["vertexes"] = [dict(base, completed="2026-09-06T06:00:02Z"), context, *batch["vertexes"]]
        self.proof(batch)

    def test_missing_duplicate_unknown_alias_and_wrong_edges_rejected(self):
        changes = [lambda b: b["vertexes"].pop(1),
                   lambda b: b["vertexes"].append(copy.deepcopy(b["vertexes"][3])),
                   lambda b: b["vertexes"].append(dict(b["vertexes"][3], digest="sha256:" + "9" * 64)),
                   lambda b: b["vertexes"][3].update(name="[build 3/3] RUN echo cached"),
                   lambda b: b["vertexes"][3].update(inputs=[b["vertexes"][0]["digest"]]),
                   lambda b: b["vertexes"][2].update(inputs=list(reversed(b["vertexes"][2]["inputs"]))),
                   lambda b: b["vertexes"][3].update(inputs=[b["vertexes"][3]["digest"]]),
                   lambda b: b["vertexes"][4].update(inputs=["sha256:" + "9" * 64]),
                   lambda b: b["vertexes"][3].update(inputs=b["vertexes"][3]["inputs"] * 2),
                   lambda b: b["vertexes"][3].update(inputs="not an array"),
                   lambda b: b["vertexes"][3].update(error="execution failed"),
                   lambda b: b["vertexes"][3].pop("completed"),
                   lambda b: b["vertexes"][3].update(started="2026-09-06T06:00:09Z"),
                   lambda b: b["vertexes"][0].update(name=b["vertexes"][0]["name"].replace("f" * 64, "e" * 64))]
        for change in changes:
            batch = self.graph(); change(batch)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.proof(batch)

    def test_intermediate_name_or_edge_drift_rejected(self):
        for field, value in (("name", "[internal] load build context"), ("inputs", [])):
            batch = self.graph(); early = copy.deepcopy(batch["vertexes"][3]); early.pop("completed")
            early[field] = value; batch["vertexes"].insert(0, early)
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.proof(batch)

    def test_operation_start_terminal_order_and_dependency_times_are_strict(self):
        for mode in ("start", "cache", "after-terminal", "dependency"):
            batch = self.graph()
            early = copy.deepcopy(batch["vertexes"][3]); early.pop("completed")
            if mode == "start":
                early["started"] = "2026-09-06T06:00:02Z"
                batch["vertexes"].insert(0, early)
            elif mode == "cache":
                early["cached"] = True; batch["vertexes"].insert(0, early)
            elif mode == "after-terminal":
                batch["vertexes"].append(early)
            else:
                batch["vertexes"][2]["completed"] = "2026-09-06T06:00:05Z"
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.proof(batch)
        for role in (2, 4):
            batch = self.graph(cached=True); batch["vertexes"][role]["cached"] = False
            with self.subTest(role=role), self.assertRaises(ValueError):
                self.proof(batch, True)

    def test_execution_marker_exact_stream_vertex_time_and_cache_state(self):
        changes = [lambda b: b.update(logs=[]),
                   lambda b: b["logs"].append(copy.deepcopy(b["logs"][0])),
                   lambda b: b["logs"][0].update(vertex=b["vertexes"][2]["digest"]),
                   lambda b: b["logs"][0].update(stream=2), lambda b: b["logs"][0].update(stream=True),
                   lambda b: b["logs"][0].update(data="not base64"),
                   lambda b: b["logs"][0].update(timestamp="2026-09-06T05:59:59Z"),
                   lambda b: b["logs"][0].update(data=base64.b64encode(b"forged\n").decode()),
                   lambda b: b["vertexes"][3].update(cached=True)]
        for change in changes:
            batch = self.graph(); change(batch)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.proof(batch)
        batch = self.graph(cached=True); batch["logs"] = self.graph()["logs"]
        with self.assertRaises(ValueError):
            self.proof(batch, True)

    def test_fragmented_marker_attached_to_run_is_accepted(self):
        batch = self.graph(); template = batch["logs"][0]
        batch["logs"] = [dict(template, data=base64.b64encode(part).decode()) for part in
                         (b"vz04-payload-", b"step-executed\n")]
        self.proof(batch)

    def test_exact_command_fixture_and_variant_binding_reject_drift(self):
        first, second = self.graph(), self.graph(cached=True, solve=1)
        a, b = self.proof(first), self.proof(second, True)
        for field in ("fixture_sha256", "dockerfile_sha256"):
            command = self.command(second, solve=1); command.build_binding[field] = "a" * 64
            with self.subTest(field=field), self.assertRaises(ValueError):
                driver.assert_payload_pair(self.command(first), a, command, b)
        for old in ("owned-machine", "owned-builder", "linux/arm64", "FIXTURE_RUN=owned-run", "FIXTURE_VARIANT=alpha", "/private/fixture"):
            command = self.command(second, solve=1)
            index = command.build_binding["argv"].index(old); command.build_binding["argv"][index] = "foreign"
            with self.subTest(argument=old), self.assertRaises(ValueError):
                driver.assert_payload_pair(self.command(first), a, command, b)
        command = self.command(first)
        expected = list(command.argv); changed = list(expected); changed[-1] = "/foreign"
        with self.assertRaises(ValueError):
            driver.bind_build_command(changed, expected, Path("/private/owned/export-0"), self.fixture_digest, b"exact Dockerfile")
        with self.assertRaises(ValueError):
            driver.bind_build_command(expected, expected, Path("/foreign"), self.fixture_digest, b"exact Dockerfile")

    def test_stale_progress_engine_clock_and_base_identity_rejected(self):
        first, second = self.graph(), self.graph(cached=True, solve=1)
        a, b = self.proof(first), self.proof(second, True)
        for field, value in (("base_vertex", "sha256:" + "7" * 64), ("progress_sha256", a["progress_sha256"]),
                             ("started_ns", a["started_ns"]), ("cached", False)):
            changed = dict(b, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                driver.assert_payload_pair(self.command(first), a, self.command(second, solve=1), changed)
        for engine in (None, True, a["started_ns"], b["started_ns"] + 1):
            command = self.command(second, solve=1); command.build_engine_ns = engine
            with self.subTest(engine=engine), self.assertRaises(ValueError):
                driver.assert_payload_pair(self.command(first), a, command, b)

    def test_other_successful_recipe_runs_must_really_execute(self):
        instruction = "RUN --network=none test ! -e /run/secrets/fixture"
        batch = {"vertexes": [{"digest": "sha256:" + "a" * 64, "name": "[build 4/4] " + instruction,
                                "completed": "2026-09-06T06:00:02Z"}]}
        driver.assert_uncached_run(self.encoded(batch), instruction)
        for changed in ({"cached": True}, {"error": "failed"}, {"completed": "2026-99-99T06:00:02Z"}):
            bad = copy.deepcopy(batch); bad["vertexes"][0].update(changed)
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                driver.assert_uncached_run(self.encoded(bad), instruction)

    def test_whole_solve_source_and_output_bounded_by_engine_observations(self):
        first, second = self.graph(), self.graph(cached=True, solve=1)
        late = copy.deepcopy(first)
        late["vertexes"][4].update(started="2030-01-01T00:00:00Z", completed="2030-01-01T00:00:01Z")
        graph = self.proof(late)
        with self.assertRaises(ValueError):
            driver.assert_payload_pair(self.command(late), graph, self.command(second, solve=1), self.proof(second, True))
        item = driver.Driver.__new__(driver.Driver)
        item.builder_guard = lambda: None
        item._engine_system_time = "2026-09-06T06:00:10Z"
        item._last_payload_graph = graph
        with self.assertRaisesRegex(ValueError, "subsequent Engine"):
            item.build("cache-cold", "Dockerfile.cache", {})
        early = copy.deepcopy(first)
        early["vertexes"][1].update(started="2026-09-06T05:59:59Z")
        with self.assertRaises(ValueError):
            driver.assert_payload_pair(self.command(early), self.proof(early), self.command(second, solve=1), self.proof(second, True))

    def test_unfinished_source_future_update_is_bounded(self):
        first, second = self.graph(), self.graph(cached=True, solve=1)
        unfinished = dict(first["vertexes"][0], started="2030-01-01T00:00:00Z")
        unfinished.pop("completed")
        first["vertexes"].append(unfinished)
        with self.assertRaises(ValueError):
            driver.assert_payload_pair(self.command(first), self.proof(first), self.command(second, solve=1), self.proof(second, True))


if __name__ == "__main__":
    unittest.main()
