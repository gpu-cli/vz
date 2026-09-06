"""Four one-shot workers rendezvous inside one explicitly owned BuildKit cache.

No networking, retry, repair, or cleanup. Failed claims remain for disposition.
Only the host-process tests supply alternate paths; the executable uses fixed paths.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

WORKERS = 4
TIMEOUT_NS = 60_000_000_000
POLL_INTERVAL_NS = 100_000_000
RELEASE_DWELL_NS = 1_000_000_000
MAX_SAMPLES = 602
MAX_RECORD_BYTES = 1024
PREFIX = "VZ_PARALLEL_BARRIER="
PHASES = ("started", "ready", "all_ready", "released", "completed")
RECORD_KEYS = {"schema_version", "run_id", "slot", "started_unix_ns",
               "started_monotonic_ns", "ready_unix_ns", "ready_monotonic_ns"}
DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class BarrierError(Exception):
    """Static public failure code; never attach untrusted paths or contents."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def monotonic_ns():
    # Explicit system-wide clock: Python 3.9 macOS monotonic_ns is relative to
    # each process, unlike Linux. Participants must share one clock origin.
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def stamp(row, phase):
    row[phase + "_unix_ns"] = time.time_ns()
    row[phase + "_monotonic_ns"] = monotonic_ns()


def names(fd, maximum):
    result = []
    with os.scandir(fd) as entries:
        for entry in entries:
            if len(result) == maximum:
                raise BarrierError("unexpected_inventory")
            result.append(entry.name)
    return set(result)


def signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise BarrierError("invalid_record")
        result[key] = value
    return result


def invalid_constant(_value):
    raise BarrierError("invalid_record")


def read_record(fd, slot, run_id):
    with os.fdopen(os.open("ready.json", os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC,
                           dir_fd=fd), "rb") as source:
        before = os.fstat(source.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_uid != os.geteuid() or not 1 <= before.st_size <= MAX_RECORD_BYTES):
            raise BarrierError("invalid_record_file")
        raw = source.read(MAX_RECORD_BYTES + 1)
        if (len(raw) != before.st_size or signature(os.fstat(source.fileno())) != signature(before)
                or signature(os.stat("ready.json", dir_fd=fd, follow_symlinks=False)) != signature(before)):
            raise BarrierError("record_changed")
    try:
        row = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise BarrierError("invalid_record") from None
    if (type(row) is not dict or set(row) != RECORD_KEYS
            or type(row["schema_version"]) is not int or row["schema_version"] != 1
            or row["run_id"] != run_id or type(row["slot"]) is not int or row["slot"] != slot):
        raise BarrierError("foreign_record")
    for clock in ("unix_ns", "monotonic_ns"):
        start, ready = row["started_" + clock], row["ready_" + clock]
        if type(start) is not int or type(ready) is not int or not 0 < start <= ready:
            raise BarrierError("invalid_record_time")
    now = monotonic_ns()
    if row["ready_monotonic_ns"] > now or now - row["started_monotonic_ns"] > TIMEOUT_NS:
        raise BarrierError("stale_record")
    return row


def observe(root, run_id):
    found = {}
    entries = names(root, WORKERS)
    if entries - {"slot-" + str(slot) for slot in range(WORKERS)}:
        raise BarrierError("unexpected_inventory")
    for slot in range(WORKERS):
        name = "slot-" + str(slot)
        if name not in entries:
            continue
        fd = os.open(name, DIRECTORY, dir_fd=root)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid():
                raise BarrierError("foreign_slot")
            contents = names(fd, 1)
            if contents - {"ready.json", "ready.pending"}:
                raise BarrierError("unexpected_inventory")
            # Publishing is one atomic rename; a listing of pending may become
            # ready immediately afterwards. That is merely a pending sample.
            if "ready.json" in contents:
                found[slot] = read_record(fd, slot, run_id)
            elif "ready.pending" in contents:
                try:
                    pending = os.stat("ready.pending", dir_fd=fd, follow_symlinks=False)
                except FileNotFoundError:
                    # The sole publisher may have atomically renamed it.
                    pending = None
                if pending is not None and (not stat.S_ISREG(pending.st_mode)
                        or pending.st_nlink != 1 or pending.st_uid != os.geteuid()
                        or pending.st_size > MAX_RECORD_BYTES):
                    raise BarrierError("invalid_record_file")
            current = os.stat(name, dir_fd=root, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                raise BarrierError("slot_changed")
        finally:
            os.close(fd)
    return found


def sample(root, row, seen):
    found = observe(root, row["run_id"])
    if any(found.get(slot) != record for slot, record in seen.items()):
        raise BarrierError("readiness_regressed")
    seen.update(found)
    if len(row["samples"]) >= MAX_SAMPLES:
        raise BarrierError("sample_limit")
    row["samples"].append({"unix_ns": time.time_ns(), "monotonic_ns": monotonic_ns(),
                           "ready_slots": sorted(found)})
    return len(found) == WORKERS


def publish(root, row):
    name = "slot-" + str(row["slot"])
    try:
        os.mkdir(name, 0o700, dir_fd=root)
    except FileExistsError:
        raise BarrierError("slot_already_claimed") from None
    fd = os.open(name, DIRECTORY, dir_fd=root)
    try:
        stamp(row, "ready")
        record = {key: row[key] for key in RECORD_KEYS}
        encoded = canonical(record)
        if len(encoded) > MAX_RECORD_BYTES:
            raise BarrierError("record_limit")
        with os.fdopen(os.open("ready.pending", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                               | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd), "wb") as target:
            target.write(encoded)
            target.flush()
            os.fsync(target.fileno())
        if names(fd, 1) != {"ready.pending"}:
            raise BarrierError("unexpected_inventory")
        os.rename("ready.pending", "ready.json", src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        os.close(fd)


def run(barrier=Path("/barrier"), output=Path("/out"), environ=None):
    env = os.environ if environ is None else environ
    row = {"schema_version": 1, "outcome": "failed", "error_code": None,
           "run_id": None, "slot": None, "generation_sha256": None,
           "participants": [], "samples": [], "payload": None}
    row.update({phase + "_" + clock: None for phase in PHASES for clock in ("unix_ns", "monotonic_ns")})
    stamp(row, "started")
    try:
        if (re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", env.get("FIXTURE_BASE", "")) is None
                or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", env.get("FIXTURE_RUN", "")) is None
                or env.get("FIXTURE_SLOT") not in ("0", "1", "2", "3")):
            raise BarrierError("invalid_environment")
        row["run_id"], row["slot"] = env["FIXTURE_RUN"], int(env["FIXTURE_SLOT"])
        root = os.open(barrier, DIRECTORY)
        try:
            observe(root, row["run_id"])
            publish(root, row)
            seen = {}
            deadline = row["started_monotonic_ns"] + TIMEOUT_NS
            while True:
                if monotonic_ns() >= deadline:
                    raise BarrierError("barrier_timeout")
                if sample(root, row, seen):
                    break
                time.sleep(min(POLL_INTERVAL_NS, max(0, deadline - monotonic_ns())) / 10**9)
            last = row["samples"][-1]
            for clock in ("unix_ns", "monotonic_ns"):
                row["all_ready_" + clock] = last[clock]
            row["participants"] = [seen[slot] for slot in range(WORKERS)]
            row["generation_sha256"] = hashlib.sha256(canonical(row["participants"])).hexdigest()
            release = row["all_ready_monotonic_ns"] + RELEASE_DWELL_NS
            while True:
                remaining = release - monotonic_ns()
                if remaining <= 0:
                    break
                time.sleep(remaining / 10**9)
            if monotonic_ns() >= deadline:
                raise BarrierError("barrier_timeout")
            sample(root, row, seen)
            stamp(row, "released")
            data = ("vz04-parallel-v1\nslot=" + str(row["slot"]) + "\n").encode()
            output = Path(output)
            output.mkdir(exist_ok=True)
            output_fd = os.open(output, DIRECTORY)
            try:
                with os.fdopen(os.open("payload.txt", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                                      | os.O_NOFOLLOW | os.O_CLOEXEC, 0o644, dir_fd=output_fd), "wb") as target:
                    target.write(data)
                    target.flush()
                    os.fchmod(target.fileno(), 0o644)
            finally:
                os.close(output_fd)
            row["payload"] = {"path": "payload.txt", "size": len(data),
                              "sha256": hashlib.sha256(data).hexdigest(), "mode": 0o644}
            stamp(row, "completed")
            if row["completed_monotonic_ns"] > deadline:
                raise BarrierError("barrier_timeout")
            row["outcome"] = "released"
        finally:
            os.close(root)
    except BarrierError as error:
        row["error_code"] = str(error)
    except OSError:
        row["error_code"] = "filesystem_error"
    return row


def main():
    row = run()
    print(PREFIX + canonical(row).decode(), flush=True)
    return 0 if row["outcome"] == "released" else 1


if __name__ == "__main__":
    sys.exit(main())
