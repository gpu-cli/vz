"""Read-only, one-shot external proof of an owned BuildKit private cgroup root.

This observes kernel state; it neither initializes cgroups nor retries readiness.
The caller must ownership-check the supplied Docker inspect record immediately
before and after capture. Raw public Exec streams are retained by its recorder.
"""
from __future__ import annotations

import base64
import hashlib
import re


LIMIT = 131072
STREAM_LIMIT = 4 * 1024 * 1024
FIELDS = (
    "before_stat", "before_membership", "before_namespace", "guest_namespace", "observer_namespace",
    "before_mountinfo", "before_process_root", "root_fs", "init_fs",
    "before_root_inode", "before_init_inode",
    "root_type", "root_controllers", "root_subtree", "root_procs",
    "init_type", "init_controllers", "init_subtree", "init_procs",
    "after_root_inode", "after_init_inode", "after_process_root",
    "after_mountinfo", "after_namespace", "after_membership", "after_stat",
)


def require(condition, message):
    if not condition:
        raise ValueError("builder cgroup: " + message)


def probe_script(pid):
    require(type(pid) is int and 1 < pid < 2**31, "invalid inspect PID")
    # All field values are base64, so newlines in proc files cannot forge frames.
    # Read LIMIT+1, not LIMIT: the validator rejects an over-limit prefix.
    lines = ["set -eu", "set -o pipefail", "bb=/bin/busybox", f"p=/proc/{pid}",
             "r=$p/root/sys/fs/cgroup", "i=$r/init",
             "printf 'VZ_BUILDKIT_CGROUP_V1\\n'",
             'read_field() { printf "%s=" "$1"; "$bb" head -c ' + str(LIMIT + 1) +
             ' "$2" | "$bb" base64 -w 0; printf "\\n"; }',
             'command_field() { n=$1; shift; printf "%s=" "$n"; "$bb" "$@" | '
             '"$bb" base64 -w 0; printf "\\n"; }']
    commands = {
        "before_stat": 'read_field before_stat "$p/stat"',
        "before_membership": 'read_field before_membership "$p/cgroup"',
        "before_namespace": 'command_field before_namespace readlink "$p/ns/cgroup"',
        "guest_namespace": 'command_field guest_namespace readlink /proc/1/ns/cgroup',
        "observer_namespace": 'command_field observer_namespace readlink /proc/self/ns/cgroup',
        "before_mountinfo": 'read_field before_mountinfo "$p/mountinfo"',
        "before_process_root": 'command_field before_process_root stat -L -c %d:%i "$p/root"',
        "root_fs": 'command_field root_fs stat -f -c %t "$r"',
        "init_fs": 'command_field init_fs stat -f -c %t "$i"',
        "before_root_inode": 'command_field before_root_inode stat -L -c %d:%i "$r"',
        "before_init_inode": 'command_field before_init_inode stat -L -c %d:%i "$i"',
    }
    for group in ("root", "init"):
        for name, filename in (("type", "cgroup.type"), ("controllers", "cgroup.controllers"),
                               ("subtree", "cgroup.subtree_control"), ("procs", "cgroup.procs")):
            commands[group + "_" + name] = f'read_field {group}_{name} "${"r" if group == "root" else "i"}/{filename}"'
    for name in ("root_inode", "init_inode", "process_root", "mountinfo", "namespace", "membership", "stat"):
        commands["after_" + name] = commands["before_" + name].replace("before_", "after_")
    lines.extend(commands[field] for field in FIELDS)
    lines.append("printf 'VZ_BUILDKIT_CGROUP_END\\n'")
    return "\n".join(lines)


def unpack(raw):
    require(type(raw) is bytes and len(raw) <= STREAM_LIMIT, "invalid or oversized stream")
    try:
        lines = raw.decode("ascii").splitlines(keepends=True)
        require(len(lines) == len(FIELDS) + 2 and lines[0] == "VZ_BUILDKIT_CGROUP_V1\n" and
                lines[-1] == "VZ_BUILDKIT_CGROUP_END\n", "missing, extra, or incomplete frames")
        result = {}
        for field, line in zip(FIELDS, lines[1:-1]):
            require(line.startswith(field + "=") and line.endswith("\n"), "field order/name drift")
            encoded = line[len(field) + 1:-1]
            value = base64.b64decode(encoded, validate=True)
            require(base64.b64encode(value).decode() == encoded and len(value) <= LIMIT,
                    "noncanonical or truncated field")
            text = value.decode("ascii")
            require(all(c in "\n\t" or 32 <= ord(c) < 127 for c in text), "control byte in field")
            result[field] = text
        return result
    except (UnicodeError, base64.binascii.Error) as error:
        raise ValueError("builder cgroup: invalid field encoding") from error


def stat_identity(value, pid):
    match = re.fullmatch(r"([1-9][0-9]*) \(([^\n]*)\) ([A-Za-z]) (.+)\n", value)
    require(match is not None and int(match[1]) == pid and match[3] not in ("Z", "X", "x"),
            "dead or mismatched process")
    rest = match[4].split(" ")
    require(len(rest) >= 19 and all(re.fullmatch(r"-?[0-9]+", v) for v in rest), "malformed process stat")
    start = int(rest[18])  # Field 22; rest starts at field 4.
    require(start > 0, "missing process birth identity")
    return {"pid": pid, "starttime_ticks": start, "comm": match[2]}


def canonical_path(value):
    require(re.fullmatch(r"/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+", value) is not None and
            all(part not in (".", "..") for part in value.split("/")[1:]), "unsafe cgroup path")
    return value


def controllers(value):
    require(value == "\n" or re.fullmatch(r"[a-z][a-z0-9_]*(?: [a-z][a-z0-9_]*)*\n", value),
            "malformed controller set")
    values = value.strip().split()
    require(len(set(values)) == len(values), "duplicate controller")
    return sorted(values)


def processes(value):
    require(not value or re.fullmatch(r"(?:[1-9][0-9]*\n)+", value), "malformed process list")
    # Kernel cgroup.procs reads need not be ordered and may repeat a PID.
    values = sorted({int(v) for v in value.splitlines()})
    require(all(v < 2**31 for v in values), "process ID out of range")
    return values


def validate(raw, pid):
    require(type(pid) is int and 1 < pid < 2**31, "invalid inspect PID")
    fields = unpack(raw)
    identity = stat_identity(fields["before_stat"], pid)
    require(stat_identity(fields["after_stat"], pid) == identity, "process identity changed")
    for field in ("membership", "namespace", "mountinfo", "process_root", "root_inode", "init_inode"):
        require(fields["before_" + field] == fields["after_" + field], field + " changed during observation")
    ns = fields["before_namespace"]
    require(re.fullmatch(r"cgroup:\[[1-9][0-9]*\]\n", ns) and
            re.fullmatch(r"cgroup:\[[1-9][0-9]*\]\n", fields["guest_namespace"]) and
            ns != fields["guest_namespace"], "builder does not have private cgroup namespace")
    require(fields["observer_namespace"] == fields["guest_namespace"], "observer is not in guest cgroup namespace")
    membership = fields["before_membership"]
    require(membership.startswith("0::") and membership.endswith("\n") and membership.count("\n") == 1,
            "expected exact unified cgroup membership")
    leaf = canonical_path(membership[3:-1])
    require(leaf.endswith("/init") and leaf != "/init", "init is not in owned nested leaf")
    root = leaf[:-5]
    mounts = []
    for line in fields["before_mountinfo"].splitlines():
        words = line.split(" ")
        require(words.count("-") == 1, "malformed mountinfo separator")
        separator = words.index("-")
        require(separator >= 6 and len(words) == separator + 4, "malformed mountinfo record")
        if words[4] == "/sys/fs/cgroup":
            mounts.append(words)
    require(len(mounts) == 1, "ambiguous or missing projected cgroup mount")
    mount = mounts[0]
    require(mount[3] == root and mount[mount.index("-") + 1] == "cgroup2" and
            "rw" in mount[5].split(","), "projected root does not match owned init membership")
    for name in ("process_root", "root_inode", "init_inode"):
        require(re.fullmatch(r"[0-9]+:[1-9][0-9]*\n", fields["before_" + name]), "invalid inode identity")
    require(fields["before_root_inode"] != fields["before_init_inode"], "init aliases root")
    require(fields["before_root_inode"].split(":")[0] == fields["before_init_inode"].split(":")[0],
            "init and root belong to different filesystems")
    require(fields["root_fs"] == fields["init_fs"] == "63677270\n", "projection is not cgroup2fs")
    require(fields["root_type"] == fields["init_type"] == "domain\n", "invalid or threaded domain")
    root_controllers = controllers(fields["root_controllers"])
    enabled = controllers(fields["root_subtree"])
    require(root_controllers and enabled == root_controllers, "root controllers not fully enabled")
    require(controllers(fields["init_controllers"]) == enabled and not controllers(fields["init_subtree"]),
            "init leaf controller topology differs")
    require(not processes(fields["root_procs"]), "root still contains processes")
    init_pids = processes(fields["init_procs"])
    require(pid in init_pids, "inspect PID absent from init leaf")
    return {"schema_version": 1, "scope": "INITIALIZED_BUILDER_ROOT_NOT_WORKLOAD_LEAF_PROOF",
            "process": identity, "namespace": ns.strip(),
            "guest_namespace": fields["guest_namespace"].strip(),
            "observer_namespace": fields["observer_namespace"].strip(), "root_path": root, "init_path": leaf,
            "root_inode": fields["before_root_inode"].strip(), "init_inode": fields["before_init_inode"].strip(),
            "root_type": "domain", "init_type": "domain", "enabled_controllers": enabled,
            "root_pids": [], "init_pids": init_pids, "stdout_sha256": hashlib.sha256(raw).hexdigest()}


def capture(harness, descriptor, inspected, label="builder-cgroup"):
    """Sample once; caller rechecks exact container ownership after this returns."""
    require(isinstance(inspected, dict) and isinstance(inspected.get("Id"), str) and
            re.fullmatch(r"[0-9a-f]{64}", inspected["Id"]),
            "invalid owned container inspect")
    require(isinstance(inspected.get("HostConfig"), dict) and isinstance(inspected.get("State"), dict) and
            inspected["HostConfig"].get("CgroupnsMode") == "private" and
            inspected["State"].get("Running") is True, "explicit private running builder required")
    pid = inspected["State"].get("Pid")
    script = probe_script(pid)
    require(isinstance(descriptor, dict) and isinstance(descriptor.get("owner"), dict), "missing Machine owner")
    owner = descriptor["owner"]
    for key in ("environment_id", "machine_id"):
        require(isinstance(owner.get(key), str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", owner[key]),
                "invalid explicit Machine owner")
    raw, stderr, code = harness.command(label, [harness.cli, "exec", "--environment", owner["environment_id"],
        "--machine", owner["machine_id"], "--no-stdin", "--timeout", "30", "--", "/bin/busybox", "sh", "-c", script],
        timeout=40, success=False)
    require(type(code) is int and code == 0 and stderr == b"", "public Exec failed; raw diagnostic retained")
    proof = validate(raw, pid)
    proof.update(container_id=inspected["Id"], owner=dict(owner), command_label=label)
    return proof
