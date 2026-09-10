#!/usr/bin/env python3
"""The checked-in deterministic agent driver for GOAL-0.4.0 criterion 12.

It drives the installed `vz` CLI the way an agent fleet does -- several workers,
each holding one Environment/Machine binding, issuing executions that carry
their own request identity -- and records every artifact the runtime produced
for every one of them.

What "deterministic" means here, precisely:

* The schedule is a checked-in document (`schedule.json`), not a program. It
  fixes the workers, the rounds, the steps in each round, each step's program
  and each step's expected outcome. The driver adds no step of its own and
  reorders nothing.
* Rounds run in declared order and never overlap: a round begins only after
  every step of the previous round has terminated. So the set of executions in
  flight at any moment is a property of the schedule alone.
* Inside a round every non-held step rendezvouses on one `threading.Barrier`
  before it spawns anything, so the steps a round declares together really do
  overlap. A round that declares a held step (a service the other steps have to
  reach) additionally waits for the runtime's own `execution_ready` event for
  that step before releasing the barrier -- the barrier is the runtime's report,
  not a sleep.
* Every identity a step uses is derived from `(run token, step id)`: its
  `--request-id`, its `--idempotency-key`, and the token its guest program
  prints. Nothing is random and nothing is order-dependent, so two runs of one
  schedule with one run token issue byte-identical command lines, and a failure
  names the step that produced it instead of a race that happened to lose.

The driver asserts nothing. It records what it INTENDED for each step (binding,
Environment selector, Machine, request identity, argv, cwd) and what the runtime
ANSWERED (every `--json` record verbatim, the raw terminal transcript for a PTY
step, the process exit status, and the wall-clock span). Judging whether the
answer is attributed to the intent is the gate check's job
(`gate.agent.deterministic_workers`), which is what keeps this fixture from
being able to certify itself.

Usage:
    driver.py --schedule <schedule.json> --plan <plan.json> --transcript <out.json>

The plan is written by the caller and binds each symbolic binding name in the
schedule to a real (project directory, Environment selector, Machine, target OS,
environment variables, program parameters). A binding the plan omits is skipped
and recorded as skipped, so a host that cannot build a native macOS Machine
produces a transcript that says so rather than a transcript that pretends.
"""
from __future__ import annotations

import argparse
import base64
import errno
import hashlib
import json
import os
from pathlib import Path
import pty
import select
import subprocess
import sys
import threading
import time

READY_RECORD = "execution_ready"
DEFAULT_WALL_TIMEOUT = 180
DEFAULT_READY_TIMEOUT = 120.0
STREAM_LIMIT = 4 * 1024 * 1024


def now_ns() -> int:
    return time.time_ns()


def substitute(argv: list, params: dict) -> list:
    """Fill `{name}` placeholders from `params`; nothing else is interpreted."""
    filled = []
    for item in argv:
        for key, value in sorted(params.items()):
            item = item.replace("{" + key + "}", str(value))
        filled.append(item)
    return filled


def identities(run_token: str, step_id: str) -> dict:
    """The three identities a step carries, derived and never random."""
    return {"request_id": f"req-{run_token}-{step_id}",
            "idempotency_key": f"idem-{run_token}-{step_id}",
            "token": f"tok-{run_token}-{step_id}"}


class Step:
    """One declared step, bound to a real Machine by the plan."""

    def __init__(self, schedule: dict, plan: dict, round_row: dict, row: dict):
        self.id = row["id"]
        self.round = round_row["index"]
        self.round_name = round_row["name"]
        self.worker = row["worker"]
        worker = next(w for w in schedule["workers"] if w["id"] == self.worker)
        self.role = worker["role"]
        self.binding_name = worker["binding"]
        self.binding = (plan.get("bindings") or {}).get(self.binding_name)
        self.channel = row.get("channel", "json")
        self.hold = bool(row.get("hold"))
        self.await_ready = row.get("await_ready")
        self.expect = row["expect"]
        self.attempts = int(row.get("attempts", 1))
        self.interval = float(row.get("interval_seconds", 0.0))
        self.timeout_seconds = row.get("timeout_seconds")
        self.wall_timeout = float(row.get("wall_timeout_seconds",
                                          plan.get("wall_timeout_seconds", DEFAULT_WALL_TIMEOUT)))
        self.identity = identities(plan["run_token"], self.id)
        self.program_name = row["program"]
        self.params = dict(row.get("params") or {})
        self.result = None
        self.ready = threading.Event()
        self.held_run = None
        if self.binding is not None:
            params = dict(self.binding.get("params") or {})
            params.update(self.params)
            target_os = self.binding["target_os"]
            program = schedule["programs"][self.program_name]
            if target_os not in program:
                raise KeyError(f"program {self.program_name} declares no {target_os} spelling")
            self.program = substitute(program[target_os], params)
            self.resolved_params = params
        else:
            self.program = []
            self.resolved_params = {}

    def argv(self, cli: str) -> list:
        binding = self.binding
        argv = [cli]
        if self.channel == "json":
            argv.append("--json")
        argv += ["exec", "--environment", binding["environment"], "--machine", binding["machine"],
                 "--request-id", self.identity["request_id"],
                 "--idempotency-key", self.identity["idempotency_key"]]
        if self.timeout_seconds is not None:
            argv += ["--timeout", str(int(self.timeout_seconds))]
        argv += ["--env", "VZ_AGENT_REQUEST=" + self.identity["request_id"],
                 "--env", "VZ_AGENT_TOKEN=" + self.identity["token"]]
        if self.channel == "pty":
            argv.append("--tty")
        else:
            argv.append("--no-stdin")
        return argv + ["--", *self.program]

    def intent(self) -> dict:
        return {"binding": self.binding_name,
                "environment": None if self.binding is None else self.binding["environment"],
                "machine": None if self.binding is None else self.binding["machine"],
                "target_os": None if self.binding is None else self.binding["target_os"],
                "cwd": None if self.binding is None else self.binding["cwd"],
                "program": self.program_name, "params": self.resolved_params,
                **self.identity}

    def skeleton(self) -> dict:
        return {"step": self.id, "round": self.round, "round_name": self.round_name, "worker": self.worker,
                "role": self.role, "channel": self.channel, "held": self.hold, "expect": self.expect,
                "intent": self.intent()}


def parse_records(data: bytes) -> tuple:
    """(records, unparsed lines). A record is one JSON object per line."""
    records, unparsed = [], []
    for line in data.decode("utf-8", "replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            unparsed.append(line[:400])
            continue
        if isinstance(row, dict):
            records.append(row)
        else:
            unparsed.append(line[:400])
    return records, unparsed


def decode_output(records: list) -> dict:
    """The guest bytes the runtime attributed to each stream, per record order."""
    streams = {"stdout": b"", "stderr": b""}
    for record in records:
        if record.get("record_type") != "execution_output":
            continue
        stream = record.get("stream") or "stdout"
        try:
            payload = base64.b64decode(record.get("base64") or "", validate=True)
        except (ValueError, TypeError):
            payload = b""
        streams[stream] = streams.get(stream, b"") + payload
    return {name: base64.b64encode(value).decode("ascii") for name, value in streams.items()}


class HeldRun:
    """A step started and left running while the rest of its round proceeds."""

    def __init__(self, step: Step, process, reader: threading.Thread, chunks: list, started: int):
        self.step = step
        self.process = process
        self.reader = reader
        self.chunks = chunks
        self.started = started


def run_json(step: Step, cli: str, environ: dict) -> dict:
    """One ordinary `--json exec`, run to completion, recorded verbatim."""
    argv = step.argv(cli)
    binding = step.binding
    result = dict(step.skeleton(), argv=argv, attempt=0)
    stdout = stderr = b""
    exit_code = None
    started = now_ns()
    for attempt in range(1, step.attempts + 1):
        result["attempt"] = attempt
        process = subprocess.Popen(argv, cwd=binding["cwd"], env=environ, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=step.wall_timeout)
            exit_code = process.returncode
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, 9)
            except (ProcessLookupError, PermissionError):
                process.kill()
            stdout, stderr = process.communicate()
            exit_code = None
            result["driver_error"] = f"the driver's own {step.wall_timeout}s wall bound expired"
        if exit_code == 0 or attempt == step.attempts:
            break
        time.sleep(step.interval)
    records, unparsed = parse_records(stdout[:STREAM_LIMIT])
    result.update(started_unix_ns=started, ended_unix_ns=now_ns(), exit_code=exit_code, records=records,
                  unparsed=unparsed, guest=decode_output(records),
                  stderr_b64=base64.b64encode(stderr[:STREAM_LIMIT]).decode("ascii"),
                  raw_b64=base64.b64encode(stdout[:STREAM_LIMIT]).decode("ascii"))
    return result


def run_pty_step(step: Step, cli: str, environ: dict) -> dict:
    """One `--tty exec` on a terminal this driver allocates.

    `vz exec --tty` refuses a stdin that is not a terminal and refuses `--json`
    with it, so a PTY step has no record stream at all: the terminal transcript
    is the only artifact, and it is what has to carry this request's identity.
    """
    argv = step.argv(cli)
    result = dict(step.skeleton(), argv=argv, attempt=1)
    master, slave = pty.openpty()
    started = now_ns()
    chunks = []
    exit_code = None
    try:
        process = subprocess.Popen(argv, cwd=step.binding["cwd"], env=environ, stdin=slave, stdout=slave,
                                   stderr=slave, start_new_session=True, close_fds=True)
        os.close(slave)
        slave = None
        deadline = time.monotonic() + step.wall_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                result["driver_error"] = f"the driver's own {step.wall_timeout}s wall bound expired"
                break
            readable, _, _ = select.select([master], [], [], min(remaining, 1.0))
            if not readable:
                if process.poll() is not None:
                    break
                continue
            try:
                data = os.read(master, 65536)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                data = b""
            if not data:
                break
            chunks.append(data)
        try:
            exit_code = process.wait(timeout=max(1.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, 9)
            except (ProcessLookupError, PermissionError):
                process.kill()
            exit_code = process.wait(timeout=10)
    finally:
        if slave is not None:
            os.close(slave)
        os.close(master)
    transcript = b"".join(chunks)[:STREAM_LIMIT]
    result.update(started_unix_ns=started, ended_unix_ns=now_ns(), exit_code=exit_code, records=[], unparsed=[],
                  guest={"stdout": base64.b64encode(transcript).decode("ascii"), "stderr": ""},
                  stderr_b64="", raw_b64=base64.b64encode(transcript).decode("ascii"),
                  terminal=True)
    return result


def start_held(step: Step, cli: str, environ: dict) -> HeldRun:
    """Start a step and leave it running; set its ready event from the stream."""
    argv = step.argv(cli)
    process = subprocess.Popen(argv, cwd=step.binding["cwd"], env=environ, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    chunks = []

    def read():
        for line in process.stdout:
            chunks.append(line)
            try:
                row = json.loads(line.decode("utf-8", "replace"))
            except ValueError:
                continue
            # The barrier the cooperating round waits on is the runtime's own
            # report that this execution is running, never a sleep: a sleep
            # would make the round's overlap a matter of timing luck.
            if isinstance(row, dict) and row.get("record_type") == READY_RECORD:
                step.ready.set()
        process.stdout.close()

    reader = threading.Thread(target=read, name="held-" + step.id, daemon=True)
    started = now_ns()
    reader.start()
    return HeldRun(step, process, reader, chunks, started)


def stop_held(run: HeldRun) -> dict:
    """End a held step deliberately and record what it produced."""
    step = run.step
    result = dict(step.skeleton(), argv=step.argv(""), attempt=1)
    result["argv"] = run.process.args if isinstance(run.process.args, list) else list(run.process.args)
    try:
        os.killpg(run.process.pid, 15)
    except (ProcessLookupError, PermissionError):
        run.process.terminate()
    try:
        run.process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(run.process.pid, 9)
        except (ProcessLookupError, PermissionError):
            run.process.kill()
        run.process.wait(timeout=30)
    run.reader.join(timeout=30)
    stderr = run.process.stderr.read() or b""
    run.process.stderr.close()
    stdout = b"".join(run.chunks)[:STREAM_LIMIT]
    records, unparsed = parse_records(stdout)
    result.update(started_unix_ns=run.started, ended_unix_ns=now_ns(), exit_code=run.process.returncode,
                  records=records, unparsed=unparsed, guest=decode_output(records),
                  stderr_b64=base64.b64encode(stderr[:STREAM_LIMIT]).decode("ascii"),
                  raw_b64=base64.b64encode(stdout).decode("ascii"),
                  ready_observed=step.ready.is_set())
    return result


def skipped(step: Step, reason: str) -> dict:
    return dict(step.skeleton(), argv=[], attempt=0, started_unix_ns=now_ns(), ended_unix_ns=now_ns(),
                exit_code=None, records=[], unparsed=[], guest={"stdout": "", "stderr": ""}, stderr_b64="",
                raw_b64="", skipped=reason)


def step_environment(plan: dict, step: Step) -> dict:
    environ = dict(plan.get("env") or {})
    environ.update(step.binding.get("env") or {})
    return {str(key): str(value) for key, value in environ.items()}


def run_round(schedule: dict, plan: dict, steps: list, results: dict) -> None:
    cli = plan["cli"]
    runnable = [step for step in steps if step.binding is not None]
    for step in steps:
        if step.binding is None:
            results[step.id] = skipped(step, f"the plan binds no {step.binding_name}")
    held = [step for step in runnable if step.hold]
    active = [step for step in runnable if not step.hold]
    running = [start_held(step, cli, step_environment(plan, step)) for step in held]
    try:
        for step in active:
            if not step.await_ready:
                continue
            waited = next((run for run in running if run.step.id == step.await_ready), None)
            if waited is not None:
                waited.step.ready.wait(DEFAULT_READY_TIMEOUT)
        if active:
            barrier = threading.Barrier(len(active))

            def execute(step=None):
                try:
                    barrier.wait(timeout=DEFAULT_READY_TIMEOUT)
                except threading.BrokenBarrierError:
                    results[step.id] = skipped(step, "the round's barrier broke before this step started")
                    return
                try:
                    runner = run_pty_step if step.channel == "pty" else run_json
                    results[step.id] = runner(step, cli, step_environment(plan, step))
                except Exception as error:  # noqa: BLE001 - recorded, never swallowed
                    results[step.id] = dict(skipped(step, f"driver error: {error!r}"))

            threads = [threading.Thread(target=execute, kwargs={"step": step}, name=step.id) for step in active]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
    finally:
        for run in running:
            results[run.step.id] = stop_held(run)


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description="vz 0.4 deterministic agent driver")
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--transcript", required=True)
    options = parser.parse_args(argv)
    schedule_bytes = Path(options.schedule).read_bytes()
    schedule = json.loads(schedule_bytes)
    plan = json.loads(Path(options.plan).read_text())
    if schedule.get("kind") != "vz-0.4-agent-schedule":
        print(f"not an agent schedule: {schedule.get('kind')!r}", file=sys.stderr)
        return 2
    ordered = []
    for round_row in schedule["rounds"]:
        ordered.append([Step(schedule, plan, round_row, row) for row in round_row["steps"]])
    results = {}
    started = now_ns()
    for steps in ordered:
        run_round(schedule, plan, steps, results)
    transcript = {
        "schema_version": 1, "kind": "vz-0.4-agent-transcript", "run_token": plan["run_token"],
        "schedule_sha256": hashlib.sha256(schedule_bytes).hexdigest(),
        "schedule_path": options.schedule, "cli": plan["cli"],
        "started_unix_ns": started, "ended_unix_ns": now_ns(),
        "rounds": [{"index": row["index"], "name": row["name"], "steps": [s["id"] for s in row["steps"]]}
                   for row in schedule["rounds"]],
        "steps": [results[step.id] for steps in ordered for step in steps],
    }
    Path(options.transcript).write_text(json.dumps(transcript, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
