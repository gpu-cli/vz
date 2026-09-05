"""Synthetic fixtures and mutation tests, not physical certification."""

import copy
import hashlib
import json
import unittest

from docker_seccomp_policy_evidence import CASES, INSPECT, PROFILE, InvalidEvidence, validate
from docker_time_namespace_evidence import NO_INPUT
from test_docker_time_namespace_evidence import CONFIG, ENDPOINT, machine_fixture


def fixture(machine=None, endpoint=ENDPOINT, config=CONFIG):
    machine = machine_fixture() if machine is None else machine
    matrix = {}

    def command(args, stdout="", exit_code=0):
        return {"args": args, "endpoint": endpoint, "config": config, "stdout": stdout,
                "stderr": "", "exit_code": exit_code, "elapsed_ms": 1,
                "input_bytes": 0, "input_sha256": NO_INPUT}

    for name, tenant, custom in CASES:
        cid = hashlib.sha256(("seccomp/" + name + json.dumps(machine["owner"], sort_keys=True)).encode()).hexdigest()
        directory = config + "/seccomp-policy-" + name
        args = ["run", "--network", "none", "--env", "LC_ALL=C", "--cidfile", directory + "/container.id"]
        if tenant:
            args += ["--detach"]
        if custom:
            args += ["--security-opt", "seccomp=" + directory + "/profile.json"]
        args += ["vz-endpoint-fixture:local", "/bin/busybox"]
        args += ["sleep", "300"] if tenant else ["readlink", "/proc/self/exe"]
        host = {"Runtime": "youki", "Privileged": False, "NetworkMode": "none",
                "ContainerIDFile": directory + "/container.id", "CapAdd": None, "CapDrop": None,
                "Devices": [], "DeviceCgroupRules": None, "SecurityOpt": ["seccomp=" + PROFILE] if custom else None}
        state = {"Running": tenant, "ExitCode": 1 if custom and not tenant else 0,
                 "Error": "", "Status": "running" if tenant else "exited", "Dead": False,
                 "Paused": False, "Restarting": False, "OOMKilled": False, "Pid": 321 if tenant else 0}
        commands = {
            "run": command(args, cid + "\n" if tenant else "" if custom else "/bin/busybox\n",
                           1 if custom and not tenant else 0),
            "inspect": command(["inspect", "--format", INSPECT, cid],
                               json.dumps({"id": cid, "state": state, "host_config": host})),
            "cleanup": command(["rm", "-f", cid], cid + "\n"),
        }
        if tenant:
            commands["exec"] = command(["exec", cid, "/bin/busybox", "readlink", "/proc/self/exe"],
                                       "" if custom else "/bin/busybox\n", 1 if custom else 0)
        matrix[name] = {"container_id": cid, "tenant": tenant, "custom_policy": custom,
                        "syscall_allowed": not custom, "numeric_errno_measured": False,
                        "cleanup_confirmed": True, "commands": commands}
    return {"schema_version": 1, "scope": "DEV_host_docker_default_and_custom_seccomp_init_exec",
            "owner": copy.deepcopy(machine["owner"]), "runtime_identity": copy.deepcopy(machine["first_identity"]),
            "endpoint": endpoint, "runtime": "youki", "profile": PROFILE,
            "profile_sha256": hashlib.sha256(PROFILE.encode()).hexdigest(), "matrix": matrix, "cleanup_confirmed": True}


class SeccompEvidenceTests(unittest.TestCase):
    def reject(self, mutate):
        value = fixture()
        mutate(value)
        with self.assertRaises(InvalidEvidence):
            validate(value, machine_fixture(), ENDPOINT, CONFIG)

    def test_exact_differential_matrix(self):
        self.assertEqual(len(set(validate(fixture(), machine_fixture(), ENDPOINT, CONFIG))), 4)

    def test_rejects_lost_exec_profile_despite_working_init(self):
        self.reject(lambda value: value["matrix"]["custom_exec"]["commands"]["exec"].update(
            exit_code=0, stdout="/bin/busybox\n"))

    def test_rejects_missing_scope_owner_profile_or_case(self):
        for key in fixture():
            self.reject(lambda value, key=key: value.pop(key))
        self.reject(lambda value: value.update(schema_version=True))
        self.reject(lambda value: value.update(runtime="runc"))
        self.reject(lambda value: value["owner"].update(machine_id="foreign"))
        self.reject(lambda value: value["runtime_identity"].update(schema_version=True))
        self.reject(lambda value: value.update(profile=PROFILE.replace("13", "1")))
        self.reject(lambda value: value.update(profile_sha256="f" * 64))
        for name, _, _ in CASES:
            self.reject(lambda value, name=name: value["matrix"].pop(name))
            for key in fixture()["matrix"][name]:
                self.reject(lambda value, name=name, key=key: value["matrix"][name].pop(key))

    def test_rejects_command_routing_overrides_failures_and_fake_cleanup(self):
        for name, _, _ in CASES:
            for command_name in fixture()["matrix"][name]["commands"]:
                for field, replacement in [("endpoint", "unix:///var/run/docker.sock"), ("config", "/tmp"),
                                           ("exit_code", True), ("stderr", "Permission denied"),
                                           ("input_bytes", 1), ("elapsed_ms", 60000)]:
                    self.reject(lambda value, name=name, command_name=command_name, field=field, replacement=replacement:
                                value["matrix"][name]["commands"][command_name].update({field: replacement}))
            self.reject(lambda value, name=name: value["matrix"][name]["commands"]["cleanup"].update(stdout="f" * 64))
            self.reject(lambda value, name=name: value["matrix"][name]["commands"]["run"]["args"].__setitem__(2, "host"))

    def test_rejects_inspect_policy_differences_and_stale_container(self):
        def inspect_change(value, name, section, field, replacement):
            command = value["matrix"][name]["commands"]["inspect"]
            inspection = json.loads(command["stdout"])
            inspection[section][field] = replacement
            command["stdout"] = json.dumps(inspection)
        for section, field, replacement in [("host_config", "Privileged", True),
                                            ("host_config", "CapAdd", ["SYS_ADMIN"]),
                                            ("host_config", "SecurityOpt", ["seccomp=unconfined"]),
                                            ("state", "Running", False), ("state", "ExitCode", True),
                                            ("state", "Error", "runtime failed"), ("state", "OOMKilled", True),
                                            ("state", "Pid", 0), ("state", "Status", "dead")]:
            self.reject(lambda value, section=section, field=field, replacement=replacement:
                        inspect_change(value, "custom_exec", section, field, replacement))
        self.reject(lambda value: inspect_change(value, "custom_exec", "host_config", "Memory", 123))


if __name__ == "__main__":
    unittest.main()
