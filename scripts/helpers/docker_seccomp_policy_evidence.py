"""Strict paired syscall evidence. Synthetic validator tests are not E2E proof."""

import hashlib
import re

from docker_time_namespace_evidence import (
    COMMAND_FIELDS, NO_INPUT, InvalidEvidence, container_id, inspect_json, keys, path, require, text,
)

PROFILE = '{"defaultAction":"SCMP_ACT_ALLOW","syscalls":[{"names":["readlinkat"],"action":"SCMP_ACT_ERRNO","errnoRet":13}]}'
INSPECT = '{"state":{{json .State}},"host_config":{{json .HostConfig}},"id":{{json .Id}}}'
CASES = (("default_init", False, False), ("custom_init", False, True),
         ("default_exec", True, False), ("custom_exec", True, True))


def validate(value, machine, endpoint, config):
    keys(value, {"schema_version", "scope", "owner", "runtime_identity", "endpoint", "runtime",
                 "profile", "profile_sha256", "matrix", "cleanup_confirmed"}, "seccomp matrix")
    require(type(value["schema_version"]) is int and value["schema_version"] == 1, "seccomp schema")
    require(value["scope"] == "DEV_host_docker_default_and_custom_seccomp_init_exec", "seccomp scope")
    require(value["owner"] == machine["owner"] and value["runtime_identity"] == machine["first_identity"],
            "seccomp stale/foreign Machine")
    keys(value["runtime_identity"], {"schema_version", "stack_id", "incarnation_id"}, "seccomp runtime identity")
    require(type(value["runtime_identity"]["schema_version"]) is int, "seccomp runtime identity version type")
    require(value["endpoint"] == endpoint and value["runtime"] == "youki", "seccomp endpoint/runtime")
    require(value["profile"] == PROFILE and value["profile_sha256"] == hashlib.sha256(PROFILE.encode()).hexdigest(),
            "custom seccomp profile mismatch")
    require(value["cleanup_confirmed"] is True, "seccomp matrix cleanup")
    keys(value["matrix"], {name for name, _, _ in CASES}, "seccomp cases")
    ids = []
    hosts = []
    for name, tenant, custom in CASES:
        case = value["matrix"][name]
        keys(case, {"container_id", "tenant", "custom_policy", "syscall_allowed",
                    "numeric_errno_measured", "commands", "cleanup_confirmed"}, name)
        cid = container_id(case["container_id"])
        ids.append(cid)
        require(case["tenant"] is tenant and case["custom_policy"] is custom
                and case["syscall_allowed"] is (not custom), "seccomp case mislabelled")
        require(case["numeric_errno_measured"] is False and case["cleanup_confirmed"] is True,
                "seccomp case overclaim/cleanup")
        commands = case["commands"]
        keys(commands, {"run", "inspect", "cleanup"} | ({"exec"} if tenant else set()), name + " commands")
        for command_name, command in commands.items():
            keys(command, COMMAND_FIELDS, name + "/" + command_name)
            require(command["endpoint"] == endpoint and command["config"] == config, "seccomp command cross-routed")
            expected_exit = 1 if custom and command_name == ("exec" if tenant else "run") else 0
            require(type(command["exit_code"]) is int and command["exit_code"] == expected_exit,
                    "seccomp command exit mismatch")
            require(type(command["elapsed_ms"]) is int and 0 <= command["elapsed_ms"] < 60000, "seccomp command deadline")
            require(type(command["input_bytes"]) is int and command["input_bytes"] == 0
                    and command["input_sha256"] == NO_INPUT, "seccomp unexpected stdin")
            text(command["stdout"], "seccomp stdout")
            require(command["stderr"] == "", "seccomp unexpected diagnostic")
        args = commands["run"]["args"]
        require(type(args) is list and len(args) >= 11, "seccomp run arguments")
        cid_path = path(args[6], "seccomp cidfile")
        require(cid_path.name == "container.id" and cid_path.parent.parent == path(config, "client config")
                and re.fullmatch(r"seccomp-policy-[A-Za-z0-9_-]{1,64}", cid_path.parent.name), "seccomp cidfile escaped")
        expected = ["run", "--network", "none", "--env", "LC_ALL=C", "--cidfile", str(cid_path)]
        if tenant:
            expected += ["--detach"]
        if custom:
            expected += ["--security-opt", "seccomp=" + str(cid_path.parent / "profile.json")]
        expected += ["vz-endpoint-fixture:local", "/bin/busybox"]
        expected += ["sleep", "300"] if tenant else ["readlink", "/proc/self/exe"]
        require(args == expected, "seccomp run changed policy/command")
        if tenant:
            require(container_id(commands["run"]["stdout"], raw=True) == cid, "seccomp run target mismatch")
            require(commands["exec"]["args"] == ["exec", cid, "/bin/busybox", "readlink", "/proc/self/exe"],
                    "seccomp exec target/command mismatch")
        observed = commands["exec" if tenant else "run"]
        require(observed["stdout"] == ("" if custom else "/bin/busybox\n"), "seccomp syscall result mismatch")
        require(commands["inspect"]["args"] == ["inspect", "--format", INSPECT, cid], "seccomp inspect target")
        require(commands["cleanup"]["args"] == ["rm", "-f", cid]
                and container_id(commands["cleanup"]["stdout"], raw=True) == cid, "seccomp exact cleanup")
        inspection = inspect_json(commands["inspect"]["stdout"])
        keys(inspection, {"state", "host_config", "id"}, "seccomp inspect")
        require(inspection["id"] == cid, "seccomp inspect wrong ID")
        state = inspection["state"]
        require(type(state) is dict and state.get("Running") is tenant, "seccomp inspect running mismatch")
        require(type(state.get("ExitCode")) is int and state["ExitCode"] == (1 if custom and not tenant else 0),
                "seccomp inspect exit mismatch")
        require(state.get("Error") == "" and state.get("Status") == ("running" if tenant else "exited"),
                "container failed outside syscall fixture")
        for flag in ("Dead", "Paused", "Restarting", "OOMKilled"):
            require(state.get(flag) is False, "seccomp invalid lifecycle flag: " + flag)
        require(type(state.get("Pid")) is int and (1 < state["Pid"] <= 2**31 - 1 if tenant else state["Pid"] == 0),
                "seccomp PID contradicts lifecycle")
        host = inspection["host_config"]
        require(type(host) is dict and host.get("Runtime") == "youki" and host.get("Privileged") is False
                and host.get("NetworkMode") == "none" and host.get("ContainerIDFile") == str(cid_path),
                "seccomp inspect runtime/config mismatch")
        for field in ("CapAdd", "CapDrop", "Devices", "DeviceCgroupRules"):
            require(field in host and host[field] in (None, []), "seccomp unexpected override: " + field)
        if custom:
            options = host.get("SecurityOpt")
            require(type(options) is list and len(options) == 1 and type(options[0]) is str
                    and options[0].startswith("seccomp="), "seccomp inspect profile missing")
            require(inspect_json(options[0][8:]) == inspect_json(PROFILE), "seccomp inspect changed profile")
        else:
            require("SecurityOpt" in host and host["SecurityOpt"] in (None, []), "default seccomp override")
        hosts.append({key: field for key, field in host.items() if key not in ("SecurityOpt", "ContainerIDFile")})
    require(len(set(ids)) == 4, "seccomp matrix reused container")
    require(all(host == hosts[0] for host in hosts), "seccomp matrix changed another HostConfig policy")
    return ids
