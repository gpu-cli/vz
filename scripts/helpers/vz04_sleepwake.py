"""Hardware sleep/wake checkpoint, operator ack and wake capture.

The persisted-recovery phase must span a real sleep of the Mac. The
orchestrator writes a checkpoint (run-id, 32-byte nonce, `kern.bootsessionuuid`,
`kern.boottime`, `CLOCK_MONOTONIC`, `CLOCK_UPTIME_RAW`, wall clock, digest of
`pmset -g log`), then blocks for the operator: Enter on the controlling TTY or
the appearance of `--sleep-wake-ack-file` containing the nonce, within the
contract's `deadlines_seconds.operator_ack`. After the ack it re-reads the
checkpoint from disk, captures the same clocks again, collects the
Sleep/Wake/DarkWake rows of `pmset -g log` and the powerd/kernel sleep-wake
messages of `log show` inside the window (bounded in bytes and time; a slow or
failing `log show` is recorded, never hidden), and computes
`discontinuity_ns = ΔMONOTONIC − ΔUPTIME_RAW`. On macOS `CLOCK_MONOTONIC`
keeps counting across sleep while `CLOCK_UPTIME_RAW` stops, so a real sleep
shows up as a positive discontinuity.

`verify(record, minimum_sleep_seconds)` is the read-only judgement shared by
the gate and the validator: same boot session, a Sleep row followed by a
Wake/DarkWake row inside the window, discontinuity at or above the minimum, and
the nonce echoed from disk.

`--dry-lanes` writes the checkpoint but never waits for an ack; the record is
`observed: false, reason: dry_lanes`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import secrets
import select
import subprocess
import sys
import time

import vz04_host as host
from vz04_common import GateError, document, load_json, sha256_bytes

PMSET = "/usr/bin/pmset"
LOG = "/usr/bin/log"
PMSET_DOMAINS = ("Sleep", "Wake", "DarkWake")
PMSET_ROW = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4}) ([^\t]*?)\s*\t(.*)$")
UNIFIED_LOG_PATTERN = re.compile(r"(?i)\b(sleep|wake|darkwake|hibernat)\w*")
MAX_UNIFIED_LOG_BYTES = 64 * 1024 * 1024
MAX_EVENTS_PER_SOURCE = 400
UNIFIED_LOG_TIMEOUT = 300
ACK_POLL_SECONDS = 0.5
REASONS = ("dry_lanes", "operator_ack_missing", "ack_deadline_exceeded", "boot_session_changed", "nonce_mismatch",
           "discontinuity_too_small", "power_events_missing", "clock_unavailable")


def clocks() -> dict:
    now = datetime.now(timezone.utc)
    return {"clock_monotonic_ns": time.clock_gettime_ns(time.CLOCK_MONOTONIC),
            "clock_uptime_raw_ns": time.clock_gettime_ns(time.CLOCK_UPTIME_RAW),
            "wall_clock_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "boot_session_uuid": host.sysctl("kern.bootsessionuuid") or "unavailable",
            "boottime_unix": host.boottime_unix() or 0}


def pmset_log() -> tuple:
    row, data = host.run_capture([PMSET, "-g", "log"], 120)
    return row, data


def parse_wall(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_pmset_rows(data: bytes) -> list:
    """[{timestamp_utc, domain, message}] for every dated row."""
    rows = []
    for line in data.decode("utf-8", "replace").splitlines():
        match = PMSET_ROW.match(line.rstrip())
        if match is None:
            continue
        try:
            stamp = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S %z").astimezone(timezone.utc)
        except ValueError:
            continue
        rows.append({"timestamp_utc": stamp.strftime("%Y-%m-%dT%H:%M:%SZ"), "domain": match.group(2).strip(),
                     "message": match.group(3).strip()[:300]})
    return rows


def pmset_power_events(data: bytes, start: datetime, end: datetime) -> list:
    events = []
    for row in parse_pmset_rows(data):
        if row["domain"] not in PMSET_DOMAINS:
            continue
        stamp = parse_wall(row["timestamp_utc"])
        if start <= stamp <= end:
            events.append({"source": "pmset -g log", "timestamp_utc": row["timestamp_utc"],
                           "event": f"{row['domain']}: {row['message']}"[:300]})
    return events[:MAX_EVENTS_PER_SOURCE]


def _local(stamp: datetime) -> str:
    return stamp.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def unified_log_events(start: datetime, end: datetime, *, timeout: int = UNIFIED_LOG_TIMEOUT, log_binary: str = LOG) -> tuple:
    """(status row, events). Bounded: the process is killed at `timeout` or
    when stdout exceeds MAX_UNIFIED_LOG_BYTES; either is recorded as status."""
    argv = [log_binary, "show", "--style", "json", "--start", _local(start - timedelta(seconds=1)), "--end", _local(end + timedelta(seconds=1)),
            "--predicate", 'subsystem == "com.apple.powerd" OR process == "kernel"']
    status = {"argv": argv, "state": "captured", "bytes": 0, "message_count": 0, "error": None, "elapsed_seconds": None}
    started = time.monotonic()
    try:
        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as error:
        status.update(state="unavailable", error=f"{type(error).__name__}: {error}")
        return status, []
    chunks, total = [], 0
    try:
        while True:
            if time.monotonic() - started > timeout:
                process.kill()
                status.update(state="timeout", error=f"log show exceeded {timeout}s; unified-log events skipped")
                break
            ready, _w, _x = select.select([process.stdout], [], [], 1.0)
            if not ready:
                if process.poll() is not None:
                    break
                continue
            chunk = os.read(process.stdout.fileno(), 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_UNIFIED_LOG_BYTES:
                process.kill()
                status.update(state="truncated", error=f"log show output exceeded {MAX_UNIFIED_LOG_BYTES} bytes; unified-log events skipped")
                break
            chunks.append(chunk)
        process.wait(timeout=30)
    except (OSError, subprocess.TimeoutExpired) as error:
        process.kill()
        status.update(state="error", error=f"{type(error).__name__}: {error}")
    status["elapsed_seconds"] = round(time.monotonic() - started, 3)
    status["bytes"] = total
    stderr = b""
    if status["state"] == "captured" and process.returncode != 0:
        try:
            stderr = process.stderr.read(4096)
        except OSError:
            stderr = b""
    process.stdout.close()
    process.stderr.close()
    if status["state"] != "captured":
        return status, []
    if process.returncode != 0:
        status.update(state="error", error=stderr.decode("utf-8", "replace").strip()[:300] or f"log show exit {process.returncode}")
        return status, []
    try:
        messages = json.loads(b"".join(chunks).decode("utf-8", "replace") or "[]")
    except json.JSONDecodeError as error:
        status.update(state="error", error=f"log show output is not JSON: {error}")
        return status, []
    if not isinstance(messages, list):
        status.update(state="error", error="log show output is not a JSON array")
        return status, []
    status["message_count"] = len(messages)
    events = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        text = str(message.get("eventMessage") or "")
        if not UNIFIED_LOG_PATTERN.search(text):
            continue
        stamp = _parse_log_timestamp(str(message.get("timestamp") or ""))
        if stamp is None or not (start - timedelta(seconds=1) <= stamp <= end + timedelta(seconds=1)):
            continue
        origin = str(message.get("subsystem") or message.get("processImagePath") or message.get("process") or "unified-log")
        events.append({"source": f"log show {origin}"[:200], "timestamp_utc": stamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "event": text[:300]})
        if len(events) >= MAX_EVENTS_PER_SOURCE:
            break
    return status, events


def _parse_log_timestamp(text: str):
    # "2026-09-06 21:02:11.123456-0600"
    match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\.\d+)?([+-]\d{4})$", text)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1) + " " + match.group(2), "%Y-%m-%d %H:%M:%S %z").astimezone(timezone.utc)
    except ValueError:
        return None


# ---------------------------------------------------------------------- checkpoint

def new_nonce() -> str:
    return secrets.token_hex(32)


def checkpoint(nonce: str) -> dict:
    row, data = pmset_log()
    value = clocks()
    value.update(nonce=nonce, pmset_log_sha256=sha256_bytes(data))
    return value


def checkpoint_path(root: Path) -> Path:
    return root / "phases" / "persisted-recovery" / "sleep-wake-checkpoint.json"


def write_checkpoint(root: Path, run_id: str, nonce: str) -> dict:
    value = checkpoint(nonce)
    path = checkpoint_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    document(path, {"schema_version": 1, "kind": "vz-0.4-sleep-wake-checkpoint", "run_id": run_id, "checkpoint": value})
    return value


# ------------------------------------------------------------------------------ ack

def _tty():
    try:
        if not os.isatty(0):
            return None
        return open("/dev/tty", "rb", buffering=0)
    except OSError:
        return None


def wait_for_ack(nonce: str, deadline_seconds: int, ack_file, *, prompt_stream=sys.stderr, clock=time.monotonic, sleep=time.sleep) -> dict:
    """Block until Enter on the controlling TTY or `ack_file` contains the nonce.

    Returns {state: acked|operator_ack_missing|ack_deadline_exceeded|nonce_mismatch,
    channel: tty|file|None, waited_seconds}.
    """
    tty = _tty()
    ack_path = Path(ack_file) if ack_file else None
    if tty is None and ack_path is None:
        return {"state": "operator_ack_missing", "channel": None, "waited_seconds": 0.0,
                "detail": "no controlling TTY and no --sleep-wake-ack-file; nobody can acknowledge the sleep"}
    started = clock()
    print(f"==> SLEEP/WAKE CHECKPOINT nonce={nonce}", file=prompt_stream, flush=True)
    print(f"==> Put this Mac to sleep for at least the contract minimum, wake it, then press Enter"
          f"{' or write the nonce to ' + str(ack_path) if ack_path else ''} (deadline {deadline_seconds}s).", file=prompt_stream, flush=True)
    try:
        while True:
            waited = clock() - started
            if waited > deadline_seconds:
                return {"state": "ack_deadline_exceeded", "channel": None, "waited_seconds": round(waited, 3),
                        "detail": f"no acknowledgement within {deadline_seconds}s"}
            if ack_path is not None and ack_path.is_file():
                try:
                    content = ack_path.read_bytes()[:4096].decode("utf-8", "replace")
                except OSError:
                    content = ""
                if nonce in content:
                    return {"state": "acked", "channel": "file", "waited_seconds": round(clock() - started, 3), "detail": str(ack_path)}
                return {"state": "nonce_mismatch", "channel": "file", "waited_seconds": round(clock() - started, 3),
                        "detail": f"{ack_path} exists but does not contain the checkpoint nonce"}
            if tty is not None:
                ready, _w, _x = select.select([tty], [], [], ACK_POLL_SECONDS)
                if ready:
                    data = tty.read(4096) or b""
                    if b"\n" in data or b"\r" in data:
                        return {"state": "acked", "channel": "tty", "waited_seconds": round(clock() - started, 3), "detail": "Enter on /dev/tty"}
            else:
                sleep(ACK_POLL_SECONDS)
    finally:
        if tty is not None:
            tty.close()


# ------------------------------------------------------------------------------ wake

def capture_wake(root: Path, checkpoint_value: dict, nonce: str) -> dict:
    """Re-read the checkpoint from disk, capture clocks and power logs."""
    on_disk = load_json(checkpoint_path(root))["checkpoint"]
    row, data = pmset_log()
    capture = clocks()
    capture.update(nonce=on_disk["nonce"], pmset_log_sha256=sha256_bytes(data))
    start, end = parse_wall(checkpoint_value["wall_clock_utc"]), parse_wall(capture["wall_clock_utc"])
    events = pmset_power_events(data, start, end)
    unified_status, unified_events = unified_log_events(start, end)
    delta_monotonic = capture["clock_monotonic_ns"] - checkpoint_value["clock_monotonic_ns"]
    delta_uptime = capture["clock_uptime_raw_ns"] - checkpoint_value["clock_uptime_raw_ns"]
    discontinuity = delta_monotonic - delta_uptime
    return {"capture": capture, "delta_monotonic_ns": max(delta_monotonic, 0), "delta_uptime_raw_ns": max(delta_uptime, 0),
            "discontinuity_ns": discontinuity, "discontinuity_seconds": max(discontinuity, 0) / 1e9,
            "nonce_echoed": on_disk["nonce"] == nonce == checkpoint_value["nonce"],
            "same_boot_session": (capture["boot_session_uuid"] == checkpoint_value["boot_session_uuid"] and
                                  capture["boottime_unix"] == checkpoint_value["boottime_unix"]),
            "power_events": events + unified_events, "unified_log": unified_status}


def paired_sleep_wake(events: list) -> bool:
    """A pmset Sleep row followed (in time order) by a Wake/DarkWake row."""
    pmset_rows = sorted((e for e in events if e["source"] == "pmset -g log"), key=lambda e: e["timestamp_utc"])
    slept = False
    for event in pmset_rows:
        domain = event["event"].split(":", 1)[0]
        if domain == "Sleep":
            slept = True
        elif domain in ("Wake", "DarkWake") and slept:
            return True
    return False


def verify(record: dict, minimum_sleep_seconds: int) -> list:
    """(code, subject, detail) findings for a sleep-wake record. Empty means bound."""
    findings = []
    subject = "phases/persisted-recovery/sleep-wake.json"
    if not record["observed"]:
        findings.append(("sleep_wake.not_observed", subject, f"hardware sleep/wake not observed: {record['reason']}"))
        return findings
    wake, check = record["wake"], record["checkpoint"]
    if not wake["same_boot_session"] or wake["capture"]["boot_session_uuid"] != check["boot_session_uuid"]:
        findings.append(("sleep_wake.boot_session", subject, "boot session changed between checkpoint and wake"))
    if not wake["nonce_echoed"] or wake["capture"]["nonce"] != check["nonce"]:
        findings.append(("sleep_wake.nonce", subject, "checkpoint nonce was not echoed by the wake capture"))
    if wake["discontinuity_seconds"] < minimum_sleep_seconds:
        findings.append(("sleep_wake.short", subject,
                         f"clock discontinuity {wake['discontinuity_seconds']:.3f}s below minimum_sleep_seconds {minimum_sleep_seconds}"))
    if not paired_sleep_wake(wake["power_events"]):
        findings.append(("sleep_wake.power_events", subject, "no pmset Sleep row followed by a Wake/DarkWake row inside the window"))
    start, end = parse_wall(check["wall_clock_utc"]), parse_wall(wake["capture"]["wall_clock_utc"])
    if end < start:
        findings.append(("sleep_wake.window", subject, "wake wall clock precedes the checkpoint"))
    for event in wake["power_events"]:
        stamp = parse_wall(event["timestamp_utc"])
        if not (start - timedelta(seconds=1) <= stamp <= end + timedelta(seconds=1)):
            findings.append(("sleep_wake.window", subject, f"power event outside the window: {event['timestamp_utc']}"))
            break
    if wake["unified_log"]["state"] != "captured":
        findings.append(("sleep_wake.unified_log", subject, f"unified log not captured: {wake['unified_log']['state']}: {wake['unified_log']['error']}"))
    return findings


# --------------------------------------------------------------------------- driver

def observe(root: Path, run_id: str, contract: dict, *, ack_file, dry: bool, prompt_stream=sys.stderr) -> Path:
    """Run checkpoint → ack → wake and write phases/persisted-recovery/sleep-wake.json."""
    sleep_contract = contract["sleep_wake"]
    minimum = sleep_contract["minimum_sleep_seconds"]
    deadline = contract["deadlines_seconds"]["operator_ack"]
    nonce = new_nonce()
    record = {"schema_version": 1, "kind": "vz-0.4-sleep-wake", "run_id": run_id, "minimum_sleep_seconds": minimum,
              "observed": False, "reason": None, "checkpoint": None, "wake": None, "ack": None}
    try:
        record["checkpoint"] = write_checkpoint(root, run_id, nonce)
    except (OSError, GateError, AttributeError) as error:
        record["reason"] = "clock_unavailable"
        record["ack"] = {"state": "not_attempted", "channel": None, "waited_seconds": 0.0, "detail": f"{type(error).__name__}: {error}"[:300]}
        return _write(root, record)
    if dry:
        record["reason"] = "dry_lanes"
        record["ack"] = {"state": "not_attempted", "channel": None, "waited_seconds": 0.0,
                         "detail": "dry-lanes developer substitution: checkpoint written, no operator ack requested"}
        return _write(root, record)
    ack = wait_for_ack(nonce, deadline, ack_file, prompt_stream=prompt_stream)
    record["ack"] = ack
    if ack["state"] != "acked":
        record["reason"] = ack["state"]
        return _write(root, record)
    record["wake"] = capture_wake(root, record["checkpoint"], nonce)
    problems = verify(dict(record, observed=True), minimum)
    codes = {code for code, _s, _d in problems}
    if "sleep_wake.boot_session" in codes:
        record["reason"] = "boot_session_changed"
    elif "sleep_wake.nonce" in codes:
        record["reason"] = "nonce_mismatch"
    elif "sleep_wake.short" in codes:
        record["reason"] = "discontinuity_too_small"
    elif "sleep_wake.power_events" in codes:
        record["reason"] = "power_events_missing"
    else:
        record["observed"] = True
    return _write(root, record)


def _write(root: Path, record: dict) -> Path:
    directory = root / "phases" / "persisted-recovery"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "sleep-wake.json"
    document(path, record)
    return path
