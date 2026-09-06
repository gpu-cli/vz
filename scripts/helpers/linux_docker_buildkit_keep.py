"""Exact-owned installed Docker probe of youki foreground --keep lifecycle.

Auxiliary direct-youki operations deliberately do not pass through the BuildKit
worker wrapper or count as BuildKit invocation evidence. No retries or repair.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import tarfile

import installed_developer_startup as startup
import linux_docker_image_input as image_input

LIMIT = 32768
CASES = (("keep-normal", True, 37, False), ("keep-force", True, 0, True),
         ("default", False, 37, False))
FIELDS = ("owner", "root_identity", "state_present", "state", "config", "cgroup_present",
          "fs", "type", "procs", "events", "cpu", "pids")
ABSENT = b"absent"


def require(value, message):
    if not value:
        raise ValueError("youki keep: " + message)


def identity(token, case):
    require(re.fullmatch(r"vzbuild-[0-9a-f]{24}", token) is not None, "invalid owned token")
    require(case in {item[0] for item in CASES}, "unknown lifecycle case")
    root = "/var/lib/buildkit/" + token + "-keep-proof"
    container_id = token + "-" + case
    return root, container_id, "/" + container_id


def configuration(token, case, code):
    root, container_id, group = identity(token, case)
    require(type(code) is int and code in (0, 37), "unexpected payload exit")
    marker = "vz-keep-payload:" + container_id
    return {"ociVersion": "1.0.2", "root": {"path": root + "/rootfs", "readonly": True},
            "process": {"terminal": False, "user": {"uid": 0, "gid": 0}, "cwd": "/",
                        "args": ["/bin/busybox", "sh", "-c", "printf '%s\\n' " + marker + "; exit " + str(code)],
                        "env": ["PATH=/bin"], "noNewPrivileges": True,
                        "capabilities": {key: [] for key in ("bounding", "effective", "inheritable", "permitted", "ambient")}},
            "mounts": [{"destination": "/proc", "type": "proc", "source": "proc",
                        "options": ["nosuid", "noexec", "nodev"]},
                       {"destination": "/dev", "type": "tmpfs", "source": "tmpfs",
                        "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]}],
            "linux": {"cgroupsPath": group,
                      "namespaces": [{"type": kind} for kind in ("mount", "pid", "ipc", "uts", "network", "cgroup")],
                      "resources": {"cpu": {"quota": 50000, "period": 100000}, "pids": {"limit": 64}}}}


def bundle(token):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for directory in ("rootfs", "rootfs/bin", "rootfs/proc", "rootfs/dev", "state", *(row[0] for row in CASES)):
            entry = tarfile.TarInfo(directory)
            entry.type, entry.mode = tarfile.DIRTYPE, 0o700
            archive.addfile(entry)
        files = {"owner": (token + "\n").encode()}
        files.update({case + "/config.json": json.dumps(configuration(token, case, code),
                      sort_keys=True, separators=(",", ":")).encode() + b"\n" for case, _, code, _ in CASES})
        for name, data in sorted(files.items()):
            entry = tarfile.TarInfo(name)
            entry.mode, entry.size = 0o600, len(data)
            archive.addfile(entry, io.BytesIO(data))
    return stream.getvalue()


def observe_script(token, case):
    root, cid, group = identity(token, case)
    # Each kernel/file field is bounded and encoded; empty cgroup.procs is valid.
    return "\n".join([
        "set -eu", "set -o pipefail", "bb=/bin/busybox", "r=" + root,
        "s=$r/state/" + cid, "g=/sys/fs/cgroup" + group,
        'test ! -L "$r"; test -d "$r"; test ! -L "$s"; test ! -L "$g"',
        "printf 'VZ_YOUKI_KEEP_V1\\n'",
        'field() { printf "%s=" "$1"; "$bb" head -c 32769 "$2" | "$bb" base64 -w 0; printf "\\n"; }',
        'value() { printf "%s=" "$1"; printf "%s" "$2" | "$bb" base64 -w 0; printf "\\n"; }',
        'field owner "$r/owner"',
        'value root_identity "$("$bb" stat -c %d:%i "$r")"',
        'if test -d "$s"; then value state_present 1; field state "$s/state.json"; field config "$s/youki_config.json"; '
        'else test ! -e "$s"; value state_present 0; value state absent; value config absent; fi',
        'if test -d "$g"; then value cgroup_present 1; value fs "$("$bb" stat -f -c %t "$g")"; '
        'field type "$g/cgroup.type"; field procs "$g/cgroup.procs"; field events "$g/cgroup.events"; '
        'field cpu "$g/cpu.max"; field pids "$g/pids.max"; '
        'else test ! -e "$g"; value cgroup_present 0; for n in fs type procs events cpu pids; do value "$n" absent; done; fi',
        "printf 'VZ_YOUKI_KEEP_END\\n'",
    ])


def observation(raw, token, case, kept, root_identity=None):
    require(type(raw) is bytes and len(raw) <= LIMIT * 16, "unbounded observation")
    lines = raw.splitlines(keepends=True)
    require(len(lines) == len(FIELDS) + 2 and lines[0] == b"VZ_YOUKI_KEEP_V1\n" and
            lines[-1] == b"VZ_YOUKI_KEEP_END\n", "incomplete observation")
    fields = {}
    for field, line in zip(FIELDS, lines[1:-1]):
        prefix = (field + "=").encode()
        require(line.startswith(prefix) and line.endswith(b"\n"), "field order or framing changed")
        encoded = line[len(prefix):-1]
        try:
            value = base64.b64decode(encoded, validate=True)
        except ValueError as error:
            raise ValueError("youki keep: malformed field encoding") from error
        require(base64.b64encode(value) == encoded and len(value) <= LIMIT, "noncanonical/oversized field")
        fields[field] = value
    root, cid, group = identity(token, case)
    require(fields["owner"] == (token + "\n").encode(), "foreign probe owner")
    require(re.fullmatch(rb"[0-9]+:[1-9][0-9]*", fields["root_identity"]), "invalid root inode")
    require(root_identity is None or fields["root_identity"].decode() == root_identity, "probe root replaced")
    if kept:
        require(fields["state_present"] == fields["cgroup_present"] == b"1", "kept state/cgroup missing")
        state, config = image_input.parse(fields["state"]), image_input.parse(fields["config"])
        require(state["id"] == cid and state["status"] == "stopped" and state.get("pid") is None and
                state["bundle"] == root + "/" + case, "kept state is not exact stopped/reaped owner")
        require(config["cgroup_path"] == group, "kept cgroup ownership changed")
        require(fields["fs"] == b"63677270" and fields["type"] == b"domain\n" and fields["procs"] == b"",
                "kept cgroup is not an empty kernel domain")
        require(fields["events"] in (b"populated 0\nfrozen 0\n", b"populated 0\n") and
                fields["cpu"] == b"50000 100000\n" and fields["pids"] == b"64\n",
                "kept cgroup is populated or resource configuration differs")
    else:
        require(fields["state_present"] == fields["cgroup_present"] == b"0" and
                all(fields[name] == ABSENT for name in ("state", "config", "fs", "type", "procs", "events", "cpu", "pids")),
                "deleted/default container left state or cgroup")
    return {"root_identity": fields["root_identity"].decode(), "state_kept": kept,
            "cgroup_kept": kept, "raw_sha256": hashlib.sha256(raw).hexdigest()}


RUN_SCRIPT = '''set -eu
expected=$1
shift
set +e
"$@"
code=$?
set -e
printf 'VZ_YOUKI_KEEP_EXIT=%s\\n' "$code"
test "$code" -eq "$expected"
'''


def run_ack(raw, error, token, case, code):
    _, cid, _ = identity(token, case)
    require(raw == ("vz-keep-payload:" + cid + "\nVZ_YOUKI_KEEP_EXIT=" + str(code) + "\n").encode()
            and error == b"", "missing exact payload marker/acknowledged exit or unexpected runtime error")


WORKER_LOG = "/var/lib/buildkit/runc-overlayfs/executor/runc-log.json"
WORKER_LOG_SCRIPT = '''set -eu
set -o pipefail
bb=/bin/busybox
for d in /var /var/lib /var/lib/buildkit /var/lib/buildkit/runc-overlayfs /var/lib/buildkit/runc-overlayfs/executor; do
  test ! -L "$d"
  if test -e "$d"; then test -d "$d"; fi
done
p=/var/lib/buildkit/runc-overlayfs/executor/runc-log.json
test ! -L "$p"
if test -e "$p"; then
  test -f "$p"
  before=$("$bb" stat -c %d:%i:%s:%Y:%Z "$p")
  printf 'present\\n%s\\n' "$before"
  "$bb" head -c 32769 "$p" | "$bb" base64 -w 0
  printf '\\n'
  after=$("$bb" stat -c %d:%i:%s:%Y:%Z "$p")
  test "$before" = "$after"
else
  printf 'absent\\n'
fi
'''


def worker_log_bytes(raw):
    """Decode complete bounded capture, distinguishing missing from an empty file."""
    require(type(raw) is bytes and len(raw) <= LIMIT * 2, "unbounded worker log frame")
    if raw == b"absent\n":
        return None
    lines = raw.splitlines(keepends=True)
    require(len(lines) == 3 and lines[0] == b"present\n" and lines[-1].endswith(b"\n"),
            "malformed worker log frame")
    require(re.fullmatch(rb"[0-9]+:[1-9][0-9]*:[0-9]+:[0-9]+:[0-9]+\n", lines[1]),
            "invalid worker log identity")
    encoded = lines[2][:-1]
    value = base64.b64decode(encoded, validate=True)
    require(len(value) <= LIMIT and base64.b64encode(value) == encoded and
            len(value) == int(lines[1].split(b":")[2]), "truncated or oversized worker log")
    return value


def verify_worker_log(builder):
    """After real BuildKit recipes, reject any error-level runtime output.

    Pinned BuildKit executor.go selects this JSON log; pinned youki
    observability.rs defaults to ERROR. Its JSON logger opens create+append before
    executing commands; go-runc forwards the configured --log path. Successful
    real workloads must therefore leave a present-empty log. Negative required-
    secret resolution does not execute a workload. Missing remains unproven.
    """
    before = builder.inspect_owned()
    raw, error, _ = builder.command("keep-post-workload-runtime-log",
        ["exec", builder.container_id, "/bin/busybox", "sh", "-c", WORKER_LOG_SCRIPT], timeout=15)
    after = builder.inspect_owned()
    require(not error and before["State"]["Pid"] == after["State"]["Pid"] and
            before["State"]["StartedAt"] == after["State"]["StartedAt"], "worker log owner lifetime changed")
    content = worker_log_bytes(raw)
    proof = {"schema_version": 1, "owner": builder.descriptor["owner"],
             "context": builder.descriptor["name"], "engine_id": builder.descriptor["engine_id"],
             "builder_id": builder.container_id, "path": WORKER_LOG,
             "runtime_sha256": builder.inventory["usr/bin/youki"]["sha256"],
             "present": content is not None, "size": len(content) if content is not None else None,
             "sha256": hashlib.sha256(content).hexdigest() if content is not None else None,
             "no_runtime_errors": content == b""}
    if content is not None:
        startup.write(builder.harness.evidence / (builder.token + "-post-workload-runtime-log.json"), content)
    startup.document(builder.harness.evidence / (builder.token + "-post-workload-runtime-log-proof.json"), proof)
    require(content is not None, "real BuildKit workload runtime log is missing; absence retained, success unproven")
    require(proof["no_runtime_errors"], "real BuildKit workload left runtime error output; raw log retained")
    return proof


def run(builder):
    """Run once; caller must keep automatic cleanup withheld until this succeeds."""
    h, token, descriptor, cid = builder.harness, builder.token, builder.descriptor, builder.container_id
    root, _, _ = identity(token, CASES[0][0])
    initial = builder.inspect_owned()
    archive = bundle(token)
    startup.write(h.evidence / (token + "-keep-bundle.tar"), archive)
    runtime_digest = builder.inventory["usr/bin/youki"]["sha256"]
    busybox_digest = builder.inventory["bin/busybox"]["sha256"]

    def guarded(label, args, mutate=False, **kwargs):
        builder.inspect_owned()
        result = builder.command("keep-" + label, args, mutate=mutate, **kwargs)
        builder.inspect_owned()
        return result

    def binaries(label):
        raw, error, _ = guarded(label, ["exec", cid, "/bin/busybox", "sha256sum", "/usr/bin/youki", "/bin/busybox"])
        require(not error and raw == (runtime_digest + "  /usr/bin/youki\n" + busybox_digest + "  /bin/busybox\n").encode(),
                "direct runtime or BusyBox differs from authenticated image")

    binaries("runtime-before")
    raw, error, _ = guarded("version", ["exec", cid, "/usr/bin/youki", "--version"])
    require(not error and b"+vz-run-keep-v1\n" in raw, "runtime lacks source-pinned keep correction")
    history_script = 'if test -f /var/lib/buildkit/vz-youki-invocations.log; then /bin/busybox head -c 32769 /var/lib/buildkit/vz-youki-invocations.log; fi'
    history, error, _ = guarded("wrapper-before", ["exec", cid, "/bin/busybox", "sh", "-c", history_script])
    require(not error and len(history) <= LIMIT, "unbounded wrapper history")
    stage = ('set -eu\nset -o pipefail\numask 077\nr=' + root + '\n'
             'test ! -e "$r"; test ! -L "$r"\n/bin/busybox mkdir "$r"\n'
             '/bin/busybox tar -x -f - -C "$r"\n'
             '/bin/busybox cp /bin/busybox "$r/rootfs/bin/busybox"\n'
             '/bin/busybox chmod 755 "$r/rootfs/bin/busybox"\n'
             '/bin/busybox sha256sum "$r/rootfs/bin/busybox"\n')
    with (h.evidence / (token + "-keep-bundle.tar")).open("rb") as archive_stream:
        raw, error, _ = guarded("stage", ["exec", "-i", cid, "/bin/busybox", "sh", "-c", stage],
                                mutate=True, stdin=archive_stream, timeout=30)
    require(not error and raw == (busybox_digest + "  " + root + "/rootfs/bin/busybox\n").encode(), "staged BusyBox changed")
    root_inode, cases = None, []
    for case, keep, code, force in CASES:
        _, nested_id, _ = identity(token, case)
        def observe(label, kept):
            raw, error, _ = guarded(case + "-" + label,
                ["exec", cid, "/bin/busybox", "sh", "-c", observe_script(token, case)], timeout=15)
            require(not error, "observation error stream")
            return observation(raw, token, case, kept, root_inode)
        before = observe("before", False)
        root_inode = before["root_identity"]
        runtime = ["/usr/bin/youki", "--root", root + "/state", "run", "--bundle", root + "/" + case]
        if keep:
            runtime.append("--keep")
        runtime.append(nested_id)
        raw, error, _ = guarded(case + "-run",
            ["exec", cid, "/bin/busybox", "sh", "-c", RUN_SCRIPT, "keep-run", str(code), *runtime],
            mutate=True, timeout=30)
        run_ack(raw, error, token, case, code)
        after_run = observe("after-run", keep)
        if keep:
            # Public runtime state projection must also omit the reaped PID.
            raw, error, _ = guarded(case + "-state", ["exec", cid, "/usr/bin/youki", "--root", root + "/state", "state", nested_id])
            state = image_input.parse(raw)
            require(not error and state["id"] == nested_id and state["status"] == "stopped" and state.get("pid") is None,
                    "public kept state is not stopped/reaped")
            args = ["exec", cid, "/usr/bin/youki", "--root", root + "/state", "delete"]
            if force:
                args.append("--force")
            raw, error, _ = guarded(case + "-delete", [*args, nested_id], mutate=True, timeout=30)
            require(not raw and not error, "explicit delete emitted an error")
        after_delete = observe("after-delete", False)
        cases.append({"case": case, "container_id": nested_id, "keep": keep, "payload_exit": code,
                      "explicit_delete": "force" if force else "normal" if keep else None,
                      "before": before, "after_run": after_run, "after_delete": after_delete})
    binaries("runtime-after")
    after_history, error, _ = guarded("wrapper-after", ["exec", cid, "/bin/busybox", "sh", "-c", history_script])
    require(not error and after_history == history, "auxiliary probe polluted BuildKit wrapper history")
    final = builder.inspect_owned()
    require(final["State"]["Pid"] == initial["State"]["Pid"] and
            final["State"]["StartedAt"] == initial["State"]["StartedAt"], "builder lifetime changed")
    proof = {"schema_version": 1, "scope": "INSTALLED_HOST_DOCKER_DIRECT_YOUKI_KEEP_NOT_BUILDKIT_WORKLOAD",
             "owner": descriptor["owner"], "context": descriptor["name"], "engine_id": descriptor["engine_id"],
             "builder_id": cid, "runtime_sha256": runtime_digest, "busybox_sha256": busybox_digest,
             "bundle_sha256": hashlib.sha256(archive).hexdigest(), "retained_fixture_root": root,
             "cases": cases, "wrapper_history_unchanged": True}
    startup.document(h.evidence / (token + "-keep-proof.json"), proof)
    return proof
