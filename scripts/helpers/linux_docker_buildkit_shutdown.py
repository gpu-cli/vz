"""Fail-closed normal-stop proof for the exact pinned BuildKit daemon.

BuildKit 0.19.0 returns the first signal's cancellation cause after
server.GracefulStop, and main consequently exits 1. Exit status alone therefore
proves neither success nor failure. This verifier needs the same process,
Engine-clock window, exact Docker events, and the pinned final error rendering.
It does not authorize cleanup of any object; the caller retains ownership guards.
"""
import calendar
import hashlib
import re
from datetime import datetime

import linux_docker_image_input as image_input

SOURCE_COMMIT = "3637d1b15a13fc3cdd0c16fcf3be0845ae68f53d"
DAEMON_SHA256 = "7dba4d777568504e8ec102d8e80223c5805b6de822366c0c44a7454178b6956d"
LABEL = "dev.vz.buildkit-proof"
EVENT_FORMAT = ('{"type":{{json .Type}},"action":{{json .Action}},'
                '"id":{{json .Actor.ID}},"attributes":{{json .Actor.Attributes}},'
                '"scope":{{json .Scope}},"time_nano":{{json .TimeNano}}}')
CAUSE = "buildkitd: got 1 SIGTERM/SIGINTs, forcing shutdown"
# Exact actual ARM64 daemon rendering, independently retained from candidate 5.
# main.go prints this cause only after GracefulStop and app.Run have returned.
TRAILER = (CAUSE, "github.com/moby/buildkit/util/appcontext.Context.func1.1",
           "\tgithub.com/moby/buildkit/util/appcontext/appcontext.go:38",
           "runtime.goexit", "\truntime/asm_arm64.s:1223")
MAX_RAW = 64 * 1024


def require(value, message):
    if not value:
        raise ValueError("builder normal stop: " + message)


def timestamp(value):
    """Strict UTC RFC3339 to integer nanoseconds, including on Python 3.9."""
    require(isinstance(value, str), "missing UTC timestamp")
    match = re.fullmatch(r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z", value)
    require(match is not None, "malformed UTC timestamp")
    parsed = datetime.fromisoformat(match[1])
    require(parsed.year > 1970, "uninitialized timestamp")
    return calendar.timegm(parsed.timetuple()) * 10**9 + int((match[2] or "").ljust(9, "0"))


def source_contract(value):
    require(value.get("buildkit_version") == "0.19.0" and value.get("source_commit") == SOURCE_COMMIT and
            value.get("buildkitd_sha256") == DAEMON_SHA256 and value.get("platform") == "linux/arm64",
            "normal-stop grammar does not match selected daemon pin")


def events(raw, container_id, container_name, image_id, token, since, until):
    require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_RAW and raw.endswith(b"\n"),
            "missing, truncated or oversized event stream")
    lines = raw.splitlines()
    require(len(lines) == 3, "expected exactly SIGTERM, die and stop events")
    found = {}
    for line in lines:
        item = image_input.parse(line)
        require(isinstance(item, dict) and set(item) == {"type", "action", "id", "attributes", "scope", "time_nano"},
                "unexpected event shape")
        require(item["type"] == "container" and item["scope"] == "local" and item["id"] == container_id,
                "foreign shutdown event")
        action = item["action"]
        require(action in ("kill", "die", "stop") and action not in found, "extra or duplicate shutdown action")
        when = item["time_nano"]
        require(type(when) is int and since <= when <= until, "event outside Engine stop window")
        attributes = item["attributes"]
        extra = {"kill": {"signal"}, "die": {"exitCode", "execDuration"}, "stop": set()}[action]
        require(isinstance(attributes, dict) and set(attributes) == {LABEL, "name", "image"} | extra and
                all(isinstance(v, str) for v in attributes.values()), "unexpected event attributes")
        require(attributes[LABEL] == token and attributes["name"] == container_name and
                attributes["image"] == image_id, "shutdown event ownership differs")
        if action == "kill":
            require(attributes["signal"] == "15", "stop was not exactly one SIGTERM")
        if action == "die":
            require(attributes["exitCode"] == "1" and re.fullmatch(r"0|[1-9][0-9]*", attributes["execDuration"]),
                    "unexpected daemon exit event")
        found[action] = item
    # Moby logs kill after task.Kill and die after SetStopped wakes stop callers.
    # Their relative ordering is not guaranteed; exact multiplicity is required.
    return found


def logs(raw, since, until):
    require(TRAILER and TRAILER[0] == CAUSE, "pinned final shutdown trailer unavailable")
    require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_RAW and raw.endswith(b"\n"),
            "missing, truncated or oversized shutdown log")
    decoded = raw.decode("utf-8", errors="strict")
    require("\r" not in decoded and "\x1b" not in decoded and "\x00" not in decoded, "invalid shutdown log bytes")
    entries, times = [], []
    for line in decoded.splitlines():
        stamp, separator, text = line.partition(" ")
        when = timestamp(stamp)
        require(separator and since <= when <= until, "log outside Engine stop window")
        require(not times or times[-1] <= when, "shutdown log timestamps go backwards")
        times.append(when)
        entries.append(text)
    require(len(entries) == 1 + len(TRAILER), "unexpected shutdown log records")
    match = re.fullmatch(r'time="([^"\n]+)" level=info msg="stopping server"', entries[0])
    # The pinned logger truncates its embedded time to seconds; Docker's outer
    # timestamps retain nanoseconds and are authoritative for the stop window.
    # Emission and Docker collection can straddle a second boundary. Bound the
    # emission's precision interval by the Engine window, not an invented
    # maximum logging delay or equality with the collection's second.
    require(match and since // 10**9 <= timestamp(match[1]) // 10**9 <= times[0] // 10**9,
            "missing normal server-stop record")
    require(tuple(entries[1:]) == TRAILER, "unexpected final daemon cause or source stack")


def validate(before, after, engine_since, engine_until, stop_elapsed_ns,
             event_bytes, log_stdout, log_stderr, token):
    since, until = timestamp(engine_since), timestamp(engine_until)
    require(0 < until - since <= 60 * 10**9, "invalid or unbounded Engine stop window")
    require(type(stop_elapsed_ns) is int and 0 < stop_elapsed_ns < 30 * 10**9,
            "stop reached force-kill deadline")
    require(before["Id"] == after["Id"] and before["Image"] == after["Image"] and before["Name"] == after["Name"],
            "stopped container identity changed")
    for item in (before, after):
        require(type(item["RestartCount"]) is int and item["RestartCount"] == 0, "builder restarted")
        state = item["State"]
        require(all(state.get(k) is False for k in ("Paused", "Restarting", "OOMKilled", "Dead")) and
                state.get("Error") == "", "abnormal builder state")
    first, last = before["State"], after["State"]
    require(first["Running"] is True and first["Status"] == "running" and
            type(first["Pid"]) is int and first["Pid"] > 0, "builder was not live before stop")
    require(last["Running"] is False and last["Status"] == "exited" and
            type(last["Pid"]) is int and last["Pid"] == 0 and
            type(last["ExitCode"]) is int and last["ExitCode"] == 1, "unexpected stopped daemon status")
    require(first["StartedAt"] == last["StartedAt"] and timestamp(first["StartedAt"]) <= since and
            since <= timestamp(last["FinishedAt"]) <= until, "stale or changed daemon lifetime")
    observed = events(event_bytes, before["Id"], before["Name"].removeprefix("/"), before["Image"], token, since, until)
    require(log_stdout == b"", "unexpected buildkitd stdout at shutdown")
    logs(log_stderr, since, until)
    return {"schema_version": 1, "scope": "PINNED_BUILDKIT_ONE_SIGTERM_NORMAL_EXIT_NOT_FILESYSTEM_CLOSURE",
            "source_commit": SOURCE_COMMIT, "buildkitd_sha256": DAEMON_SHA256,
            "container_id": before["Id"], "pid_before_stop": first["Pid"],
            "started_at": first["StartedAt"], "finished_at": last["FinishedAt"],
            "engine_since": engine_since, "engine_until": engine_until, "stop_elapsed_ns": stop_elapsed_ns,
            "exit_code": 1, "signal": "SIGTERM", "events": observed,
            "events_sha256": hashlib.sha256(event_bytes).hexdigest(),
            "stdout_sha256": hashlib.sha256(log_stdout).hexdigest(),
            "stderr_sha256": hashlib.sha256(log_stderr).hexdigest()}
