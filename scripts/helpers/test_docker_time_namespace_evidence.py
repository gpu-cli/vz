"""Synthetic fixtures and adversarial checks; these are not physical evidence."""

import copy
import hashlib
import json
import unittest

from docker_time_namespace_evidence import (
    INSPECT, InvalidEvidence, NO_INPUT, ROOT_EXEC_SCRIPT, root_guest_script, validate,
)


ENDPOINT = "unix:///private/tmp/vz-de-fixture/a.sock"
CONFIG = "/private/tmp/vz-de-fixture/client"


def machine_fixture():
    return {"owner": {"project_id": "prj_fixture", "environment_id": "env_fixture", "machine_id": "mch_a"},
            "first_identity": {"schema_version": 1, "stack_id": "vzr1-runtime-fixture",
                               "incarnation_id": "00000000-0000-4000-8000-000000000001"}}


def fixture(machine=None, endpoint=ENDPOINT, config=CONFIG):
    """Build one synthetic proof, optionally bound to the main validator fixture."""
    machine = machine_fixture() if machine is None else machine
    cid = hashlib.sha256(json.dumps(machine["owner"], sort_keys=True).encode()).hexdigest()
    pid = 321
    namespace = "time:[4026533001]"
    guest = "time:[4026531834]"
    cid_path = config + "/time-ns-fixture/container.id"

    def command(args, stdout):
        return {"args": args, "endpoint": endpoint, "config": config, "exit_code": 0,
                "stdout": stdout, "stderr": "", "elapsed_ms": 1,
                "input_bytes": 0, "input_sha256": NO_INPUT}

    state = {"Status": "running", "Running": True, "Paused": False, "Restarting": False,
             "OOMKilled": False, "Dead": False, "Pid": pid, "ExitCode": 0, "Error": "",
             "StartedAt": "2026-09-05T00:00:00Z", "FinishedAt": "0001-01-01T00:00:00Z"}
    root_observation = {"script": root_guest_script(pid), "stdout":
                        "35:917575\n26:2\npid:[4026532231]\nmnt:[4026532305]\npid:[4026531836]\nmnt:[4026531841]\n"}
    return {
        "schema_version": 1, "scope": "host_docker_default_time_namespace_and_exec_only",
        "time_offsets_tested": False, "namespace_overrides_used": False,
        "owner": copy.deepcopy(machine["owner"]), "runtime_identity": copy.deepcopy(machine["first_identity"]),
        "endpoint": endpoint, "container_id": cid, "container_init_pid": pid, "runtime": "youki",
        "container_init_time_namespace": namespace, "exec_time_namespace": namespace,
        "guest_init_time_namespace": guest, "container_time_namespace_isolated": True,
        "exec_joined_container_time_namespace": True, "cleanup_confirmed": True,
        "root_filesystem": {
            "schema_version": 1, "scope": "host_docker_exec_container_root_pid_and_mount_boundary",
            "container_id": cid, "container_init_pid": pid,
            "guest_before": copy.deepcopy(root_observation), "guest_after": copy.deepcopy(root_observation),
            "exec": command(["exec", cid, "/bin/busybox", "sh", "-c", ROOT_EXEC_SCRIPT],
                            "35:917575\n35:917575\npid:[4026532231]\nmnt:[4026532305]\npid:[4026532231]\nmnt:[4026532305]\n"),
            "exec_root_matches_container_init": True, "exec_proc_matches_container_namespaces": True,
        },
        "commands": {
            "run": command(["run", "--detach", "--network", "none", "--cidfile", cid_path,
                            "vz-endpoint-fixture:local", "/bin/busybox", "sleep", "60"], cid + "\n"),
            "inspect": command(["inspect", "--format", INSPECT, cid],
                               json.dumps({"state": state, "runtime": "youki", "id": cid}) + "\n"),
            "exec": command(["exec", cid, "/bin/busybox", "readlink", "/proc/self/ns/time"], namespace + "\n"),
            "cleanup": command(["rm", "-f", cid], cid + "\n"),
        },
        "guest_observations": [
            {"script": f"/bin/busybox readlink /proc/{pid}/ns/time", "stdout": namespace + "\n"},
            {"script": "/bin/busybox readlink /proc/1/ns/time", "stdout": guest + "\n"},
        ],
    }


class EvidenceTests(unittest.TestCase):
    def reject(self, mutate):
        value = fixture()
        mutate(value)
        with self.assertRaises(InvalidEvidence):
            validate(value, machine_fixture(), ENDPOINT, CONFIG)

    def test_fixture_binds_machine_endpoint_and_returns_exact_id(self):
        machine = machine_fixture()
        value = fixture(machine)
        self.assertEqual(validate(value, machine, ENDPOINT, CONFIG), value["container_id"])
        other = copy.deepcopy(machine)
        other["owner"]["machine_id"] = "mch_b"
        other["first_identity"]["incarnation_id"] = "00000000-0000-4000-8000-000000000002"
        endpoint = ENDPOINT.replace("a.sock", "b.sock")
        second = fixture(other, endpoint=endpoint, config=CONFIG)
        self.assertNotEqual(validate(second, other, endpoint, CONFIG), value["container_id"])

    def test_root_receipt_rejects_observed_escape_despite_matching_namespaces(self):
        self.reject(lambda value: value["root_filesystem"]["exec"].update(
            stdout=value["root_filesystem"]["exec"]["stdout"].replace("35:917575", "2:1")))
        self.reject(lambda value: value["root_filesystem"]["exec"].update(
            stdout=value["root_filesystem"]["exec"]["stdout"].replace("pid:[4026532231]", "pid:[4026531836]")))

    def test_root_receipt_rejects_missing_forged_cross_routed_or_changed_observations(self):
        for name in fixture()["root_filesystem"]:
            with self.subTest(missing=name):
                self.reject(lambda value, name=name: value["root_filesystem"].pop(name))
        for field, replacement in [("schema_version", True), ("container_id", "f" * 64),
                                   ("container_init_pid", 322), ("container_init_pid", True),
                                   ("exec_root_matches_container_init", 1), ("exec_proc_matches_container_namespaces", False)]:
            self.reject(lambda value, field=field, replacement=replacement:
                        value["root_filesystem"].update({field: replacement}))
        for field, replacement in [("endpoint", "unix:///var/run/docker.sock"), ("config", "/tmp"),
                                   ("exit_code", True), ("exit_code", 1), ("stderr", "Permission denied"),
                                   ("input_bytes", 1), ("input_sha256", "0" * 64), ("elapsed_ms", 60000)]:
            self.reject(lambda value, field=field, replacement=replacement:
                        value["root_filesystem"]["exec"].update({field: replacement}))
        self.reject(lambda value: value["root_filesystem"]["guest_after"].update(stdout="changed"))
        self.reject(lambda value: value["root_filesystem"]["guest_before"].update(script=root_guest_script(322)))
        self.reject(lambda value: value["root_filesystem"]["exec"]["args"].__setitem__(1, "f" * 64))

    def test_root_receipt_rejects_malformed_or_overflowing_kernel_ids(self):
        def change_root(value, replacement):
            proof = value["root_filesystem"]
            for name in ("guest_before", "guest_after", "exec"):
                proof[name]["stdout"] = proof[name]["stdout"].replace("35:917575", replacement)
        for bad in ("", "35:0", "035:917575", "35:01", "-1:2", "35:18446744073709551616", "35:2:3"):
            with self.subTest(root=bad):
                self.reject(lambda value, bad=bad: change_root(value, bad))

    def test_exact_schema_and_honest_scope(self):
        for name in list(fixture()):
            with self.subTest(missing=name):
                self.reject(lambda value, name=name: value.pop(name))
        for name, replacement in [("schema_version", True), ("schema_version", 2),
                                  ("scope", "full_docker_compatibility"), ("time_offsets_tested", True),
                                  ("namespace_overrides_used", True), ("cleanup_confirmed", 1),
                                  ("container_time_namespace_isolated", False),
                                  ("exec_joined_container_time_namespace", False), ("extra", True)]:
            with self.subTest(name=name):
                self.reject(lambda value: value.update({name: replacement}))
        self.reject(lambda value: value["commands"].update(info=copy.deepcopy(value["commands"]["inspect"])))

    def test_wrong_owner_runtime_generation_and_endpoint(self):
        mutations = [lambda value: value["owner"].update(machine_id="foreign"),
                     lambda value: value["runtime_identity"].update(incarnation_id="00000000-0000-4000-8000-000000000002"),
                     lambda value: value["runtime_identity"].update(schema_version=True),
                     lambda value: value.update(endpoint="unix:///var/run/docker.sock"),
                     lambda value: value.update(runtime="runc")]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                self.reject(mutate)
        for endpoint, config in [("tcp://localhost:2375", CONFIG), (ENDPOINT, CONFIG + "/../client"),
                                 ("unix:///var/run/docker.sock", "/var/run/client")]:
            with self.subTest(endpoint=endpoint), self.assertRaises(InvalidEvidence):
                validate(fixture(endpoint=endpoint, config=config), machine_fixture(), endpoint, config)

    def test_all_command_metadata_is_required_and_bounded(self):
        for name in ("run", "inspect", "exec", "cleanup"):
            for field, replacement in [("endpoint", ENDPOINT.replace("a.sock", "b.sock")),
                                       ("config", "/private/tmp/foreign"), ("elapsed_ms", True),
                                       ("elapsed_ms", -1), ("elapsed_ms", 60_000), ("exit_code", 1),
                                       ("exit_code", False), ("input_bytes", True), ("input_bytes", 1),
                                       ("input_sha256", "0" * 64), ("stdout", "x" * 65_537),
                                       ("stderr", "x" * 65_537), ("stderr", "\0"), ("args", "run"),
                                       ("extra", 1)]:
                with self.subTest(name=name, field=field, replacement=str(replacement)[:20]):
                    self.reject(lambda value: value["commands"][name].update({field: replacement}))

    def test_no_runtime_namespace_or_broad_cleanup_overrides(self):
        for name in ("run", "inspect", "exec", "cleanup"):
            for flag in ("--runtime=runc", "--pid=host", "--privileged", "--all", "--context=default"):
                with self.subTest(name=name, flag=flag):
                    self.reject(lambda value: value["commands"][name]["args"].append(flag))
        for cid_path in ("/tmp/container.id", CONFIG + "/time-ns-../container.id",
                         CONFIG + "/time-ns-fixture/../container.id", CONFIG + "/container.id"):
            with self.subTest(cid_path=cid_path):
                self.reject(lambda value: value["commands"]["run"]["args"].__setitem__(5, cid_path))
        self.reject(lambda value: value["commands"]["run"]["args"].__setitem__(3, "host"))
        self.reject(lambda value: value["commands"]["exec"]["args"].__setitem__(1, "f" * 64))
        self.reject(lambda value: value["commands"]["cleanup"]["args"].__setitem__(2, "f" * 64))

    def test_ids_and_cleanup_require_one_full_matching_container(self):
        for malformed in ("a" * 12, "F" * 64, "x" * 64, "a" * 64 + "\n" + "b" * 64):
            for name in ("run", "cleanup"):
                with self.subTest(name=name, malformed=malformed):
                    self.reject(lambda value: value["commands"][name].update(stdout=malformed))
        self.reject(lambda value: value["commands"]["cleanup"].update(stdout="f" * 64))
        self.reject(lambda value: value.update(container_id="f" * 64))

    def test_inspect_binds_actual_runtime_running_state_pid_and_id(self):
        def altered(value, field, replacement):
            data = json.loads(value["commands"]["inspect"]["stdout"])
            if field.startswith("state."):
                data["state"][field[6:]] = replacement
            else:
                data[field] = replacement
            value["commands"]["inspect"]["stdout"] = json.dumps(data)
        for field, replacement in [("runtime", "runc"), ("id", "f" * 64), ("state.Running", 1),
                                   ("state.Running", False), ("state.Pid", True), ("state.Pid", 322),
                                   ("state.Pid", 321.0), ("state.Dead", True), ("state.Status", "exited"),
                                   ("state.ExitCode", 1), ("state.Error", "failed"), ("extra", None)]:
            with self.subTest(field=field):
                self.reject(lambda value: altered(value, field, replacement))
        for pid in (True, 0, 1, -1, 2**31, "321"):
            with self.subTest(pid=pid):
                self.reject(lambda value: value.update(container_init_pid=pid))
        for raw in ("{}", "[]", "not JSON", '{"id":1,"id":2}', '{"state":{"Pid":NaN}}'):
            with self.subTest(raw=raw):
                self.reject(lambda value: value["commands"]["inspect"].update(stdout=raw))

    def test_raw_namespace_observations_not_success_flags_establish_isolation(self):
        for malformed in ("time:[0]", "time:[01]", "time:[18446744073709551616]", "time:[-1]",
                          "net:[4026533001]", "time:[4026533001]\nnoise", "time:[1]\ntime:[2]"):
            with self.subTest(malformed=malformed):
                self.reject(lambda value: value["commands"]["exec"].update(stdout=malformed))
        for name in ("container_init_time_namespace", "exec_time_namespace", "guest_init_time_namespace"):
            with self.subTest(name=name):
                self.reject(lambda value: value.update({name: "time:[999]"}))
        self.reject(lambda value: value["guest_observations"][0].update(script="/bin/busybox readlink /proc/1/ns/time"))
        self.reject(lambda value: value["guest_observations"][1].update(script="printf 'time:[4026531834]'"))
        self.reject(lambda value: value["guest_observations"].reverse())
        self.reject(lambda value: value["guest_observations"][0].update(stdout="time:[999]"))

        def same_namespace(value):
            value["guest_init_time_namespace"] = value["container_init_time_namespace"]
            value["guest_observations"][1]["stdout"] = value["guest_observations"][0]["stdout"]
        self.reject(same_namespace)


if __name__ == "__main__":
    unittest.main()
