"""Synthetic device-proof fixtures and adversarial validation, not physical proof."""

import copy
import hashlib
import json
import unittest

from docker_device_policy_evidence import DENIED, IMAGE, INSPECT, NODES, RULE, SETUP, InvalidEvidence, validate
from docker_time_namespace_evidence import NO_INPUT
from test_docker_time_namespace_evidence import CONFIG, ENDPOINT, machine_fixture


def fixture(machine=None, endpoint=ENDPOINT, config=CONFIG):
    machine = machine_fixture() if machine is None else machine
    matrix = {}
    for allowed, name in [(False, "default_policy"), (True, "explicit_device_rule_control")]:
        cid = hashlib.sha256((json.dumps(machine["owner"], sort_keys=True) + name).encode()).hexdigest()
        cidfile = config + "/device-policy-" + name + "/container.id"
        def command(args, stdout, stderr="", code=0):
            return {"args": args, "stdout": stdout, "stderr": stderr, "exit_code": code,
                    "endpoint": endpoint, "config": config, "input_bytes": 0, "input_sha256": NO_INPUT, "elapsed_ms": 1}
        args = ["run", "--detach", "--network", "none", "--env", "LC_ALL=C", "--cidfile", cidfile]
        if allowed:
            args += ["--device-cgroup-rule", RULE]
        args += [IMAGE, "/bin/busybox", "sleep", "300"]
        host = {"Runtime": "youki", "Privileged": False, "NetworkMode": "none", "CapAdd": None,
                "CapDrop": None, "SecurityOpt": None, "Devices": [], "DeviceRequests": None,
                "DeviceCgroupRules": [RULE] if allowed else None, "ContainerIDFile": cidfile,
                "ReadonlyRootfs": False, "Memory": 0}
        state = {"Running": True, "Pid": 322 if allowed else 321, "Status": "running", "Paused": False,
                 "Restarting": False, "OOMKilled": False, "Dead": False, "ExitCode": 0, "Error": ""}
        matrix[name] = {
            "container_id": cid, "device_cgroup_rule": RULE if allowed else None,
            "cap_eff": 0xa80425fb, "seccomp": 2, "cap_mknod_observed": True,
            "init_cap_eff": 0xa80425fb, "exec_cap_eff": 0xa80425fb, "init_seccomp": 2, "exec_seccomp": 2,
            "null_create_and_write": True, "loop_control_node_created": True, "loop_control_major": 10, "loop_control_minor": 237,
            "loop_control_open_allowed": allowed, "errno_symbolic": None if allowed else "EPERM",
            "numeric_errno_measured": False, "cleanup_confirmed": True,
            "commands": {
                "run": command(args, cid + "\n"),
                "inspect": command(["inspect", "--format", INSPECT, cid], json.dumps({"id": cid, "state": state, "host_config": host}) + "\n"),
                "init_status": command(["exec", cid, "/bin/busybox", "grep", "-E", "^(CapEff|Seccomp):", "/proc/1/status"], "CapEff:\t00000000a80425fb\nSeccomp:\t2\n"),
                "exec_status": command(["exec", cid, "/bin/busybox", "grep", "-E", "^(CapEff|Seccomp):", "/proc/self/status"], "CapEff:\t00000000a80425fb\nSeccomp:\t2\n"),
                "node_creation_and_null": command(["exec", cid, "/bin/busybox", "sh", "-c", SETUP], NODES),
                "loop_control_open": command(["exec", cid, "/bin/busybox", "dd", "if=/dev/vz-policy-loop-control", "of=/dev/null", "count=0"], "", "0+0 records in\n0+0 records out\n" if allowed else DENIED, 0 if allowed else 1),
                "cleanup": command(["rm", "-f", cid], cid + "\n"),
            },
        }
    return {"schema_version": 2, "scope": "DEV_host_docker_differential_device_open_policy",
            "owner": copy.deepcopy(machine["owner"]), "runtime_identity": copy.deepcopy(machine["first_identity"]),
            "endpoint": endpoint, "runtime": "youki", "sole_policy_difference": {"device_cgroup_rule": RULE},
            "privileged_or_runtime_overrides": False, "default_capabilities_and_seccomp": True,
            "matrix": matrix, "default_device_open_policy_enforced": True,
            "numeric_errno_measured": False, "cleanup_confirmed": True}


class DeviceEvidenceTests(unittest.TestCase):
    def reject(self, mutate):
        value = fixture()
        mutate(value)
        with self.assertRaises(InvalidEvidence):
            validate(value, machine_fixture(), ENDPOINT, CONFIG)

    def test_complete_pair_returns_distinct_exact_ids(self):
        value = fixture()
        ids = validate(value, machine_fixture(), ENDPOINT, CONFIG)
        self.assertEqual(ids, [case["container_id"] for case in value["matrix"].values()])
        self.assertEqual(len(set(ids)), 2)

    def test_schema_scope_and_claims_are_exact(self):
        for field in fixture():
            with self.subTest(missing=field):
                self.reject(lambda value: value.pop(field))
        for field, replacement in [("schema_version", True), ("schema_version", 1), ("scope", "full_docker_conformance"),
                                   ("runtime", "runc"), ("numeric_errno_measured", True),
                                   ("numeric_errno", 1), ("cleanup_confirmed", 1),
                                   ("default_device_open_policy_enforced", False),
                                   ("privileged_or_runtime_overrides", True),
                                   ("default_capabilities_and_seccomp", False)]:
            with self.subTest(field=field):
                self.reject(lambda value: value.update({field: replacement}))
        self.reject(lambda value: value["matrix"].pop("explicit_device_rule_control"))

    def test_rejects_old_runtime_default_allowed_tun_target(self):
        for case in ("default_policy", "explicit_device_rule_control"):
            self.reject(lambda value: value["matrix"][case].update(loop_control_minor=200))
            self.reject(lambda value: value["matrix"][case]["commands"]["loop_control_open"]["args"].__setitem__(
                4, "if=/dev/vz-policy-tun"))
        self.reject(lambda value: value.update(sole_policy_difference={"device_cgroup_rule": "c 10:200 rwm"}))

    def test_machine_endpoint_generation_are_exact(self):
        for mutate in [lambda value: value["owner"].update(machine_id="foreign"),
                       lambda value: value["runtime_identity"].update(incarnation_id="00000000-0000-4000-8000-000000000002"),
                       lambda value: value["runtime_identity"].update(schema_version=True),
                       lambda value: value.update(endpoint="unix:///var/run/docker.sock")]:
            self.reject(mutate)

    def test_every_command_requires_exact_routing_and_bounded_raw_evidence(self):
        for case in fixture()["matrix"]:
            for name in fixture()["matrix"][case]["commands"]:
                for field, replacement in [("endpoint", "unix:///var/run/docker.sock"), ("config", "/Users/user/.docker"),
                                           ("elapsed_ms", True), ("elapsed_ms", -1), ("elapsed_ms", 60000),
                                           ("input_bytes", 1), ("input_bytes", False), ("input_sha256", "f" * 64),
                                           ("stdout", "x" * 65537), ("stderr", "\0"), ("exit_code", False),
                                           ("args", "run"), ("extra", True)]:
                    with self.subTest(case=case, command=name, field=field):
                        self.reject(lambda value: value["matrix"][case]["commands"][name].update({field: replacement}))

    def test_run_has_no_hidden_runtime_privilege_or_capability_override(self):
        for case in fixture()["matrix"]:
            for flag in ["--privileged", "--runtime=runc", "--cap-add=ALL", "--security-opt=seccomp=unconfined", "--context=default"]:
                self.reject(lambda value: value["matrix"][case]["commands"]["run"]["args"].append(flag))
            for index, replacement in [(3, "host"), (5, "LC_ALL=en_US.UTF-8"), (7, CONFIG + "/../container.id")]:
                self.reject(lambda value: value["matrix"][case]["commands"]["run"]["args"].__setitem__(index, replacement))

    def test_setup_and_open_are_actual_observations_not_printf(self):
        for case in fixture()["matrix"]:
            self.reject(lambda value: value["matrix"][case]["commands"]["node_creation_and_null"]["args"].__setitem__(-1, "printf fabricated"))
            self.reject(lambda value: value["matrix"][case]["commands"]["node_creation_and_null"].update(stdout="character special file:1:3\ncharacter special file:1:3\n"))
            self.reject(lambda value: value["matrix"][case]["commands"]["loop_control_open"]["args"].__setitem__(-1, "count=1"))
            self.reject(lambda value: value["matrix"][case].update(loop_control_major=True))
            self.reject(lambda value: value["matrix"][case].update(null_create_and_write=False))

    def test_denial_requires_exact_eperm_not_merely_nonzero_exit(self):
        for stderr in ["", "Permission denied\n", "No such file or directory\n", DENIED + "extra", DENIED.replace("Operation not permitted", "Invalid argument")]:
            self.reject(lambda value: value["matrix"]["default_policy"]["commands"]["loop_control_open"].update(stderr=stderr))
        for code in [0, 2, 125, True]:
            self.reject(lambda value: value["matrix"]["default_policy"]["commands"]["loop_control_open"].update(exit_code=code))
        self.reject(lambda value: value["matrix"]["default_policy"].update(errno_symbolic="EACCES"))
        self.reject(lambda value: value["matrix"]["default_policy"].update(numeric_errno=1))
        self.reject(lambda value: value["matrix"]["explicit_device_rule_control"]["commands"]["loop_control_open"].update(exit_code=1, stderr=DENIED))

    def test_capability_seccomp_requires_raw_matching_default_observations(self):
        for raw in ["", "CapEff:\t00000000a00425fb\nSeccomp:\t2\n", "CapEff:\t00000000a80425fb\nSeccomp:\t0\n",
                    "CapEff:\t00000000a80425fb\nSeccomp:\t2\nSeccomp:\t2\n", "CapEff:\tfffffffffffffffff\nSeccomp:\t2\n",
                    "CapEff:\t00000000a80425fb\nSeccomp:\t2\nunknown"]:
            for phase in ["init_status", "exec_status"]:
                self.reject(lambda value: value["matrix"]["default_policy"]["commands"][phase].update(stdout=raw))
        self.reject(lambda value: value["matrix"]["default_policy"].update(cap_eff=1 << 27))
        self.reject(lambda value: value["matrix"]["default_policy"].update(seccomp=True))
        def changed_pair(value):
            case = value["matrix"]["explicit_device_rule_control"]
            case["cap_eff"] = 1 << 27
            for phase in ["init", "exec"]:
                case[phase + "_cap_eff"] = 1 << 27
                case["commands"][phase + "_status"]["stdout"] = "CapEff:\t0000000008000000\nSeccomp:\t2\n"
        self.reject(changed_pair)

    def test_init_seccomp_cannot_certify_unfiltered_exec_or_wrong_observation_target(self):
        for case_name in fixture()["matrix"]:
            def unfiltered_exec(value):
                case = value["matrix"][case_name]
                case["exec_seccomp"] = 0
                case["commands"]["exec_status"]["stdout"] = "CapEff:\t00000000a80425fb\nSeccomp:\t0\n"
            self.reject(unfiltered_exec)
            self.reject(lambda value: value["matrix"][case_name]["commands"]["exec_status"]["args"].__setitem__(-1, "/proc/1/status"))
            self.reject(lambda value: value["matrix"][case_name]["commands"]["init_status"]["args"].__setitem__(-1, "/proc/self/status"))
            self.reject(lambda value: value["matrix"][case_name].update(exec_cap_eff=True))

    @staticmethod
    def inspect_change(value, case, field, replacement):
        command = value["matrix"][case]["commands"]["inspect"]
        inspected = json.loads(command["stdout"])
        if field.startswith("host."):
            inspected["host_config"][field[5:]] = replacement
        elif field.startswith("state."):
            inspected["state"][field[6:]] = replacement
        else:
            inspected[field] = replacement
        command["stdout"] = json.dumps(inspected)

    def test_raw_inspect_cannot_hide_nondefault_policy_or_wrong_container(self):
        for field, replacement in [("host.Runtime", "runc"), ("host.Privileged", True), ("host.CapAdd", ["ALL"]),
                                   ("host.CapDrop", ["MKNOD"]), ("host.SecurityOpt", ["seccomp=unconfined"]),
                                   ("host.Devices", [{"PathOnHost": "/dev/net/tun"}]), ("host.DeviceCgroupRules", [RULE]),
                                   ("host.DeviceRequests", [{}]), ("host.Memory", 999), ("host.ContainerIDFile", "/tmp/foreign"),
                                   ("state.Running", 1), ("state.Pid", True), ("state.Dead", True), ("id", "f" * 64)]:
            with self.subTest(field=field):
                self.reject(lambda value: self.inspect_change(value, "default_policy", field, replacement))
        for raw in ["{}", "[]", "not JSON", '{"id":1,"id":2}', '{"host_config":{"Memory":NaN}}']:
            self.reject(lambda value: value["matrix"]["default_policy"]["commands"]["inspect"].update(stdout=raw))

    def test_cleanup_must_remove_each_exact_full_container(self):
        for case in fixture()["matrix"]:
            for raw in ["a" * 12, "F" * 64, "f" * 64, "a" * 64 + "\n" + "b" * 64]:
                self.reject(lambda value: value["matrix"][case]["commands"]["cleanup"].update(stdout=raw))
            self.reject(lambda value: value["matrix"][case]["commands"]["cleanup"].update(args=["container", "prune", "-f"]))
            self.reject(lambda value: value["matrix"][case].update(cleanup_confirmed=False))

    def test_pair_cannot_reuse_container_or_cidfile_even_with_consistent_raw_claims(self):
        def same_container(value):
            control = value["matrix"]["explicit_device_rule_control"]
            old = control["container_id"]
            replacement = value["matrix"]["default_policy"]["container_id"]
            value["matrix"]["explicit_device_rule_control"] = json.loads(json.dumps(control).replace(old, replacement))
        self.reject(same_container)
        def same_cidfile(value):
            control = value["matrix"]["explicit_device_rule_control"]
            old = control["commands"]["run"]["args"][7]
            replacement = value["matrix"]["default_policy"]["commands"]["run"]["args"][7]
            value["matrix"]["explicit_device_rule_control"] = json.loads(json.dumps(control).replace(old, replacement))
        self.reject(same_cidfile)


if __name__ == "__main__":
    unittest.main()
