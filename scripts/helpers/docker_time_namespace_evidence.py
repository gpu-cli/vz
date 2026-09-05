"""Validate default Docker time-namespace/exec evidence, not offset conformance.

The enclosing host-endpoint validator must independently check Engine info's
DefaultRuntime=youki and reject duplicate returned container IDs across Machines.
This module binds the actual container runtime and every command to one Machine.
"""

import hashlib
import json
from pathlib import PurePosixPath
import re
import uuid


class InvalidEvidence(ValueError):
    """Evidence does not establish the precisely scoped physical behavior."""


IMAGE = "vz-endpoint-fixture:local"
INSPECT = '{"state":{{json .State}},"runtime":{{json .HostConfig.Runtime}},"id":{{json .Id}}}'
NO_INPUT = hashlib.sha256(b"").hexdigest()
COMMAND_FIELDS = {"args", "endpoint", "config", "exit_code", "stdout", "stderr",
                  "input_bytes", "input_sha256", "elapsed_ms"}
FIELDS = {"schema_version", "scope", "time_offsets_tested", "namespace_overrides_used",
          "owner", "runtime_identity", "endpoint", "container_id", "container_init_pid",
          "runtime", "container_init_time_namespace", "exec_time_namespace",
          "guest_init_time_namespace", "container_time_namespace_isolated",
          "exec_joined_container_time_namespace", "commands", "guest_observations",
          "cleanup_confirmed", "root_filesystem"}

ROOT_EXEC_SCRIPT = "set -eu; /bin/busybox stat -Lc '%d:%i' / /proc/self/root; /bin/busybox readlink /proc/self/ns/pid; /bin/busybox readlink /proc/self/ns/mnt; /bin/busybox readlink /proc/1/ns/pid; /bin/busybox readlink /proc/1/ns/mnt"


def root_guest_script(pid):
    return f"set -eu; /bin/busybox stat -Lc '%d:%i' /proc/{pid}/root /; /bin/busybox readlink /proc/{pid}/ns/pid; /bin/busybox readlink /proc/{pid}/ns/mnt; /bin/busybox readlink /proc/1/ns/pid; /bin/busybox readlink /proc/1/ns/mnt"


def validate_root(value, cid, pid, endpoint, config):
    """Root inode and proc view must match init, not merely its namespaces."""
    keys(value, {"schema_version", "scope", "container_id", "container_init_pid",
                 "guest_before", "exec", "guest_after", "exec_root_matches_container_init",
                 "exec_proc_matches_container_namespaces"}, "root boundary")
    require(type(value["schema_version"]) is int and value["schema_version"] == 1, "root schema")
    require(value["scope"] == "host_docker_exec_container_root_pid_and_mount_boundary", "root scope")
    require(value["container_id"] == cid and type(value["container_init_pid"]) is int
            and value["container_init_pid"] == pid, "root target mismatch")
    require(value["exec_root_matches_container_init"] is True
            and value["exec_proc_matches_container_namespaces"] is True, "root boundary not proved")
    for name in ("guest_before", "guest_after"):
        keys(value[name], {"script", "stdout"}, name)
        require(value[name]["script"] == root_guest_script(pid), "wrong init root observation")
        text(value[name]["stdout"], name, 1024)
    require(value["guest_before"] == value["guest_after"], "root/namespace identity changed during exec")
    command = value["exec"]
    keys(command, COMMAND_FIELDS, "root exec")
    require(command["args"] == ["exec", cid, "/bin/busybox", "sh", "-c", ROOT_EXEC_SCRIPT], "wrong root exec")
    require(command["endpoint"] == endpoint and command["config"] == config, "root exec cross-routed")
    require(type(command["exit_code"]) is int and command["exit_code"] == 0, "root exec failed")
    require(type(command["elapsed_ms"]) is int and 0 <= command["elapsed_ms"] < 60_000, "root exec deadline")
    require(type(command["input_bytes"]) is int and command["input_bytes"] == 0
            and command["input_sha256"] == NO_INPUT, "unexpected root exec stdin")
    text(command["stdout"], "root exec stdout", 1024)
    require(command["stderr"] == "", "root exec diagnostic error")
    before = value["guest_before"]["stdout"].splitlines()
    executed = command["stdout"].splitlines()
    require(len(before) == 6 and len(executed) == 6, "incomplete root/proc observations")

    def root(identity):
        match = re.fullmatch(r"(0|[1-9][0-9]*):([1-9][0-9]*)", identity)
        require(match is not None, "noncanonical root identity")
        require(int(match[1]) <= 2**64 - 1 and int(match[2]) <= 2**64 - 1, "root identity overflow")

    def ns(identity, kind):
        match = re.fullmatch(kind + r":\[([1-9][0-9]*)\]", identity)
        require(match is not None and int(match[1]) <= 2**64 - 1, "invalid root namespace identity")

    for identity in (before[0], before[1], executed[0], executed[1]):
        root(identity)
    require(before[0] != before[1] and executed[0] == executed[1] == before[0], "exec escaped container root")
    for kind, target, guest, actual, visible_init in (
            ("pid", before[2], before[4], executed[2], executed[4]),
            ("mnt", before[3], before[5], executed[3], executed[5])):
        for identity in (target, guest, actual, visible_init):
            ns(identity, kind)
        require(target != guest and target == actual == visible_init, "exec proc view or namespace escaped container")


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def keys(value, expected, label):
    require(type(value) is dict and set(value) == set(expected), label + " schema mismatch")


def text(value, label, limit=65_536):
    require(type(value) is str, label + " must be text")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as error:
        raise InvalidEvidence(label + " is not UTF-8") from error
    require(size <= limit and "\0" not in value, label + " exceeds bounds or contains NUL")
    return value


def path(value, label):
    text(value, label, 4096)
    result = PurePosixPath(value)
    require(result.is_absolute() and ".." not in result.parts and str(result) == value
            and not any(ord(char) < 32 for char in value), label + " is not a canonical absolute path")
    return result


def container_id(value, raw=False):
    text(value, "container ID", 256)
    value = value.strip() if raw else value
    require(re.fullmatch(r"[0-9a-f]{64}", value) is not None, "expected exactly one full container ID")
    return value


def namespace(value, raw=False):
    text(value, "time namespace", 128)
    value = value.strip() if raw else value
    match = re.fullmatch(r"time:\[([1-9][0-9]*)\]", value)
    require(match is not None, "invalid/noncanonical time namespace")
    require(int(match[1]) <= 2**64 - 1, "time namespace inode overflow")
    return value


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        require(name not in result, "duplicate inspect JSON key")
        result[name] = value
    return result


def inspect_json(value):
    try:
        return json.loads(value, object_pairs_hook=unique_object,
                          parse_constant=lambda value: (_ for _ in ()).throw(
                              InvalidEvidence("nonfinite inspect JSON number")))
    except (ValueError, TypeError, RecursionError) as error:
        raise InvalidEvidence("invalid inspect JSON: " + str(error)) from error


def validate(value, machine, expected_endpoint, expected_config):
    """Validate one proof and return its exact ID for cross-Machine uniqueness."""
    keys(value, FIELDS, "time namespace proof")
    require(type(machine) is dict and "owner" in machine and "first_identity" in machine,
            "missing expected Machine identity")
    require(type(value["schema_version"]) is int and value["schema_version"] == 1, "proof schema version")
    require(value["scope"] == "host_docker_default_time_namespace_and_exec_only", "time namespace scope overclaim")
    for name in ("time_offsets_tested", "namespace_overrides_used"):
        require(value[name] is False, name + " must remain false")
    for name in ("container_time_namespace_isolated", "exec_joined_container_time_namespace", "cleanup_confirmed"):
        require(value[name] is True, name + " was not established")

    keys(value["owner"], {"project_id", "environment_id", "machine_id"}, "owner")
    for identity in value["owner"].values():
        text(identity, "owner identity", 512)
        require(identity and identity.strip() == identity, "empty/noncanonical owner identity")
    require(value["owner"] == machine["owner"], "wrong Machine owner")
    identity = value["runtime_identity"]
    keys(identity, {"schema_version", "stack_id", "incarnation_id"}, "runtime identity")
    require(type(identity["schema_version"]) is int and identity["schema_version"] == 1, "runtime identity schema")
    text(identity["stack_id"], "runtime stack ID", 512)
    require(identity["stack_id"], "empty runtime stack ID")
    text(identity["incarnation_id"], "runtime incarnation", 36)
    try:
        incarnation = uuid.UUID(identity["incarnation_id"])
    except ValueError as error:
        raise InvalidEvidence("invalid runtime incarnation") from error
    require(incarnation.version == 4 and str(incarnation) == identity["incarnation_id"], "noncanonical runtime incarnation")
    require(identity == machine["first_identity"], "stale or foreign runtime generation")

    text(expected_endpoint, "expected endpoint", 4096)
    require(expected_endpoint.startswith("unix:///private/tmp/vz-de-"), "expected endpoint is not a private host Unix socket")
    socket = path(expected_endpoint[7:], "expected socket")
    config = path(expected_config, "expected client config")
    require(socket.suffix == ".sock" and config == socket.parent / "client", "endpoint/client configuration scope mismatch")
    require(value["endpoint"] == expected_endpoint, "proof endpoint mismatch")
    cid = container_id(value["container_id"])
    pid = value["container_init_pid"]
    require(type(pid) is int and 1 < pid <= 2**31 - 1, "invalid container init PID")
    require(value["runtime"] == "youki", "actual container runtime is not youki")

    commands = value["commands"]
    keys(commands, {"run", "inspect", "exec", "cleanup"}, "Docker commands")
    for name, command in commands.items():
        keys(command, COMMAND_FIELDS, name + " command")
        require(command["endpoint"] == expected_endpoint and command["config"] == expected_config,
                name + " command cross-routed")
        require(type(command["args"]) is list and 0 < len(command["args"]) <= 16, "invalid command arguments")
        for arg in command["args"]:
            text(arg, "command argument", 4096)
        require(type(command["exit_code"]) is int and command["exit_code"] == 0, name + " command failed")
        require(type(command["elapsed_ms"]) is int and 0 <= command["elapsed_ms"] < 60_000,
                name + " command exceeded deadline")
        require(type(command["input_bytes"]) is int and command["input_bytes"] == 0
                and command["input_sha256"] == NO_INPUT, "unexpected command stdin")
        text(command["stdout"], name + " stdout")
        text(command["stderr"], name + " stderr")

    run = commands["run"]
    args = run["args"]
    require(len(args) == 10, "default run argument count changed")
    cid_path = path(args[5], "cidfile")
    require(cid_path.name == "container.id" and cid_path.parent.parent == config
            and re.fullmatch(r"time-ns-[A-Za-z0-9_-]{1,64}", cid_path.parent.name) is not None,
            "cidfile escaped its private per-proof directory")
    require(args == ["run", "--detach", "--network", "none", "--cidfile", str(cid_path),
                     IMAGE, "/bin/busybox", "sleep", "60"], "run changed default runtime or namespace semantics")
    require(container_id(run["stdout"], raw=True) == cid, "run returned another container")
    require(commands["inspect"]["args"] == ["inspect", "--format", INSPECT, cid], "inspect command changed")
    inspection = inspect_json(commands["inspect"]["stdout"])
    keys(inspection, {"state", "runtime", "id"}, "container inspect")
    require(inspection["id"] == cid and inspection["runtime"] == "youki", "inspect container/runtime mismatch")
    state = inspection["state"]
    require(type(state) is dict and {"Running", "Pid"} <= set(state)
            and set(state) <= {"Status", "Running", "Paused", "Restarting", "OOMKilled", "Dead", "Pid",
                               "ExitCode", "Error", "StartedAt", "FinishedAt"}, "unexpected Docker State schema")
    require(state["Running"] is True and type(state["Pid"]) is int and state["Pid"] == pid,
            "inspect did not identify the running container init")
    for flag in ("Paused", "Restarting", "OOMKilled", "Dead"):
        require(flag not in state or state[flag] is False, "container State contradicts running proof")
    require("Status" not in state or state["Status"] == "running", "container is not running")
    require("ExitCode" not in state or type(state["ExitCode"]) is int and state["ExitCode"] == 0,
            "container has failed exit status")
    require("Error" not in state or state["Error"] == "", "container State contains error")
    for timestamp in ("StartedAt", "FinishedAt"):
        if timestamp in state:
            text(state[timestamp], timestamp, 128)

    require(commands["exec"]["args"] == ["exec", cid, "/bin/busybox", "readlink", "/proc/self/ns/time"],
            "exec command changed target or namespace observation")
    require(commands["cleanup"]["args"] == ["rm", "-f", cid], "cleanup is not exact-container removal")
    require(container_id(commands["cleanup"]["stdout"], raw=True) == cid, "cleanup removed another container")
    observations = value["guest_observations"]
    require(type(observations) is list and len(observations) == 2, "missing guest namespace observations")
    for observation, script in zip(observations, [f"/bin/busybox readlink /proc/{pid}/ns/time",
                                                "/bin/busybox readlink /proc/1/ns/time"]):
        keys(observation, {"script", "stdout"}, "guest observation")
        require(observation["script"] == script, "guest namespace observation script changed")
    init = namespace(observations[0]["stdout"], raw=True)
    guest = namespace(observations[1]["stdout"], raw=True)
    executed = namespace(commands["exec"]["stdout"], raw=True)
    require(init == namespace(value["container_init_time_namespace"])
            and guest == namespace(value["guest_init_time_namespace"])
            and executed == namespace(value["exec_time_namespace"]), "raw/claimed namespace mismatch")
    require(executed == init and init != guest, "default time isolation or exec namespace join failed")
    validate_root(value["root_filesystem"], cid, pid, expected_endpoint, expected_config)
    return cid
