"""Strict differential device-open proof; not broad Docker/device conformance.

Raw C-locale BusyBox stderr establishes symbolic EPERM, not a numeric errno
measurement. The positive dd count=0 still opens its input, without read/ioctl.
"""

import re
import uuid

from docker_time_namespace_evidence import (
    COMMAND_FIELDS, NO_INPUT, InvalidEvidence, container_id, inspect_json, keys,
    path, require, text,
)

IMAGE = "vz-endpoint-fixture:local"
RULE = "c 10:237 rwm"
INSPECT = '{"state":{{json .State}},"host_config":{{json .HostConfig}},"id":{{json .Id}}}'
SETUP = """set -eu
/bin/busybox mknod /dev/vz-policy-null c 1 3
/bin/busybox mknod /dev/vz-policy-loop-control c 10 237
/bin/busybox stat -c '%F:%t:%T' /dev/vz-policy-null /dev/vz-policy-loop-control
printf baseline >/dev/null
printf baseline >/dev/vz-policy-null
"""
NODES = "character special file:1:3\ncharacter special file:a:ed\n"
DENIED = "dd: can't open '/dev/vz-policy-loop-control': Operation not permitted\n"
FIELDS = {"schema_version", "scope", "owner", "runtime_identity", "endpoint", "runtime",
          "sole_policy_difference", "privileged_or_runtime_overrides",
          "default_capabilities_and_seccomp", "matrix", "default_device_open_policy_enforced",
          "numeric_errno_measured", "cleanup_confirmed"}
CASE_FIELDS = {"container_id", "device_cgroup_rule", "cap_eff", "seccomp", "cap_mknod_observed",
               "init_cap_eff", "exec_cap_eff", "init_seccomp", "exec_seccomp",
               "null_create_and_write", "loop_control_node_created", "loop_control_major", "loop_control_minor",
               "loop_control_open_allowed", "errno_symbolic", "numeric_errno_measured", "commands",
               "cleanup_confirmed"}


def capability_status(raw):
    text(raw, "raw capability/seccomp status")
    observations = {}
    for line in raw.splitlines():
        match = re.fullmatch(r"(CapEff|Seccomp):\t([0-9a-fA-F]+)", line)
        require(match is not None and match[1] not in observations,
                "missing, duplicate or invalid capability/seccomp observation")
        observations[match[1]] = match[2]
    require(set(observations) == {"CapEff", "Seccomp"}, "missing capability/seccomp observation")
    require(len(observations["CapEff"]) == 16 and observations["Seccomp"] == "2",
            "invalid effective capability width or inactive seccomp filter")
    caps = int(observations["CapEff"], 16)
    require(caps & (1 << 27) != 0, "CAP_MKNOD is absent")
    return caps, 2


def validate_case(value, allowed, endpoint, config):
    keys(value, CASE_FIELDS, "device policy case")
    cid = container_id(value["container_id"])
    require(value["device_cgroup_rule"] == (RULE if allowed else None), "wrong device rule")
    for name in ("cap_mknod_observed", "null_create_and_write", "loop_control_node_created", "cleanup_confirmed"):
        require(value[name] is True, name + " not established")
    require(type(value["loop_control_major"]) is int and value["loop_control_major"] == 10
            and type(value["loop_control_minor"]) is int and value["loop_control_minor"] == 237,
            "wrong character device identity")
    require(value["loop_control_open_allowed"] is allowed, "wrong device-open outcome")
    require(value["errno_symbolic"] == (None if allowed else "EPERM")
            and value["numeric_errno_measured"] is False, "fabricated errno evidence")
    commands = value["commands"]
    keys(commands, {"run", "inspect", "init_status", "exec_status", "node_creation_and_null", "loop_control_open", "cleanup"},
         "device policy commands")
    for name, command in commands.items():
        keys(command, COMMAND_FIELDS, name + " command")
        require(command["endpoint"] == endpoint and command["config"] == str(config),
                "device command escaped exact Machine/client configuration")
        require(type(command["args"]) is list and 0 < len(command["args"]) <= 16,
                "invalid device command arguments")
        for arg in command["args"]:
            text(arg, "device command argument", 4096)
        expected_code = 1 if name == "loop_control_open" and not allowed else 0
        require(type(command["exit_code"]) is int and command["exit_code"] == expected_code,
                name + " unexpected exit code")
        require(type(command["elapsed_ms"]) is int and 0 <= command["elapsed_ms"] < 60_000,
                "device command exceeded bound")
        require(type(command["input_bytes"]) is int and command["input_bytes"] == 0
                and command["input_sha256"] == NO_INPUT, "unexpected device command stdin")
        text(command["stdout"], name + " stdout")
        text(command["stderr"], name + " stderr")

    args = commands["run"]["args"]
    require(len(args) == (14 if allowed else 12), "device run argument count changed")
    cidfile = path(args[7], "device cidfile")
    require(cidfile.name == "container.id" and cidfile.parent.parent == config
            and re.fullmatch(r"device-policy-[A-Za-z0-9_-]{1,64}", cidfile.parent.name),
            "device cidfile escaped private case directory")
    expected = ["run", "--detach", "--network", "none", "--env", "LC_ALL=C", "--cidfile", str(cidfile)]
    if allowed:
        expected += ["--device-cgroup-rule", RULE]
    expected += [IMAGE, "/bin/busybox", "sleep", "300"]
    require(args == expected, "device pair changed more than its exact cgroup rule")
    require(container_id(commands["run"]["stdout"], raw=True) == cid, "run ID mismatch")
    require(commands["inspect"]["args"] == ["inspect", "--format", INSPECT, cid], "inspect target changed")
    inspection = inspect_json(commands["inspect"]["stdout"])
    keys(inspection, {"state", "host_config", "id"}, "device inspect")
    require(inspection["id"] == cid, "inspect returned foreign container")
    state = inspection["state"]
    require(type(state) is dict and state.get("Running") is True
            and type(state.get("Pid")) is int and 1 < state["Pid"] <= 2**31 - 1,
            "device container is not running")
    for flag in ("Paused", "Restarting", "OOMKilled", "Dead"):
        require(flag not in state or state[flag] is False, "device state contradicts running proof")
    require("Status" not in state or state["Status"] == "running", "device state not running")
    require("Error" not in state or state["Error"] == "", "device state contains error")
    require("ExitCode" not in state or type(state["ExitCode"]) is int and state["ExitCode"] == 0,
            "device container has failed exit code")
    host = inspection["host_config"]
    required = {"Runtime", "Privileged", "NetworkMode", "CapAdd", "CapDrop", "SecurityOpt", "Devices", "DeviceCgroupRules"}
    require(type(host) is dict and required <= set(host), "incomplete HostConfig")
    require(host["Runtime"] == "youki" and host["Privileged"] is False and host["NetworkMode"] == "none",
            "runtime/privilege/network override")
    for field in ("CapAdd", "CapDrop", "SecurityOpt", "Devices"):
        require(host[field] is None or host[field] == [], "nondefault " + field)
    require(host["DeviceCgroupRules"] == [RULE] if allowed else host["DeviceCgroupRules"] in (None, []),
            "inspect device rule differs from exact request")
    if "DeviceRequests" in host:
        require(host["DeviceRequests"] is None or host["DeviceRequests"] == [], "unexpected device request")
    normalized_host = dict(host)
    normalized_host["DeviceCgroupRules"] = None
    if "ContainerIDFile" in host:
        require(host["ContainerIDFile"] in ("", str(cidfile)), "foreign HostConfig cidfile")
        normalized_host["ContainerIDFile"] = ""

    observations = []
    for phase, target in [("init", "/proc/1/status"), ("exec", "/proc/self/status")]:
        command = commands[phase + "_status"]
        require(command["args"] == ["exec", cid, "/bin/busybox", "grep", "-E", "^(CapEff|Seccomp):", target],
                phase + " capability observation command changed")
        caps, seccomp = capability_status(command["stdout"])
        require(type(value[phase + "_cap_eff"]) is int and value[phase + "_cap_eff"] == caps
                and type(value[phase + "_seccomp"]) is int and value[phase + "_seccomp"] == seccomp,
                phase + " claimed capability/seccomp differs from raw observation")
        observations.append((caps, seccomp))
    require(observations[0] == observations[1], "init and exec capability/seccomp differ")
    caps, seccomp = observations[0]
    require(type(value["cap_eff"]) is int and value["cap_eff"] == caps
            and type(value["seccomp"]) is int and value["seccomp"] == seccomp,
            "claimed capability/seccomp differs from raw observation")
    require(commands["node_creation_and_null"]["args"] == ["exec", cid, "/bin/busybox", "sh", "-c", SETUP]
            and commands["node_creation_and_null"]["stdout"] == NODES,
            "node creation, identity or successful null baseline missing")
    require(commands["loop_control_open"]["args"] == ["exec", cid, "/bin/busybox", "dd", "if=/dev/vz-policy-loop-control", "of=/dev/null", "count=0"],
            "device open command changed")
    require(commands["loop_control_open"]["stdout"] == "", "unexpected device-open stdout")
    if not allowed:
        require(commands["loop_control_open"]["stderr"] == DENIED, "default open did not report exact C-locale EPERM")
    require(commands["cleanup"]["args"] == ["rm", "-f", cid]
            and container_id(commands["cleanup"]["stdout"], raw=True) == cid,
            "cleanup is not exact successful container removal")
    return cid, cidfile, caps, seccomp, normalized_host


def validate(value, machine, expected_endpoint, expected_config):
    """Bind both cases to one exact Machine; return IDs for aggregate uniqueness."""
    keys(value, FIELDS, "device policy proof")
    require(type(value["schema_version"]) is int and value["schema_version"] == 2, "device proof version")
    require(value["scope"] == "DEV_host_docker_differential_device_open_policy", "device scope overclaim")
    require(value["runtime"] == "youki" and value["sole_policy_difference"] == {"device_cgroup_rule": RULE},
            "device proof changed runtime or policy differential")
    require(value["privileged_or_runtime_overrides"] is False and value["numeric_errno_measured"] is False,
            "unproven privilege or numeric errno claim")
    for name in ("default_capabilities_and_seccomp", "default_device_open_policy_enforced", "cleanup_confirmed"):
        require(value[name] is True, name + " was not established")
    keys(value["owner"], {"project_id", "environment_id", "machine_id"}, "device owner")
    require(value["owner"] == machine["owner"], "foreign device proof owner")
    identity = value["runtime_identity"]
    keys(identity, {"schema_version", "stack_id", "incarnation_id"}, "device runtime identity")
    require(type(identity["schema_version"]) is int and identity["schema_version"] == 1
            and identity == machine["first_identity"], "foreign/stale device runtime generation")
    try:
        incarnation = uuid.UUID(identity["incarnation_id"])
    except (ValueError, TypeError, AttributeError) as error:
        raise InvalidEvidence("invalid device incarnation") from error
    require(incarnation.version == 4 and str(incarnation) == identity["incarnation_id"], "invalid device incarnation")
    text(expected_endpoint, "device endpoint", 4096)
    require(expected_endpoint.startswith("unix:///private/tmp/vz-de-"), "device endpoint escaped private fixture")
    socket = path(expected_endpoint[7:], "device socket")
    config = path(expected_config, "device config")
    require(socket.suffix == ".sock" and config == socket.parent / "client"
            and value["endpoint"] == expected_endpoint, "wrong device endpoint/config")
    keys(value["matrix"], {"default_policy", "explicit_device_rule_control"}, "device matrix")
    baseline = validate_case(value["matrix"]["default_policy"], False, expected_endpoint, config)
    control = validate_case(value["matrix"]["explicit_device_rule_control"], True, expected_endpoint, config)
    require(baseline[0] != control[0] and baseline[1] != control[1], "device pair reused container or cidfile")
    require(baseline[2:] == control[2:], "device pair changed capabilities/seccomp/HostConfig beyond one rule")
    return [baseline[0], control[0]]
