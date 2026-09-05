#!/usr/bin/env python3
"""DEV installed Delete observer-disconnect proof, not crash/release certification.

Fresh fixtures only. A reaped observer followed by a fresh durable Running
snapshot is mandatory; missing that window fails without retry. Completion is
observed through read-only SQLite before any exact-request replay is sent.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time
import uuid

import installed_delete_e2e as base
import installed_developer_startup as startup
from installed_delete_state import DeleteStateReader

require = startup.require
SCOPE = "DEV_INSTALLED_PUBLIC_DELETE_OBSERVER_DISCONNECT_NOT_RELEASE_CERTIFICATION"


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for name in startup.OPTIONS:
        require(sum(value == "--" + name or value.startswith("--" + name + "=") for value in argv) <= 1,
                "duplicate option: --" + name)
        parser.add_argument("--" + name, required=True)
    return parser.parse_args(argv)


def preflight(args, require_host=True):
    info = base.preflight(args, require_host=require_host)
    info["scope"] = SCOPE
    for name in ("scripts/helpers/installed_delete_disconnect_e2e.py",
                 "scripts/helpers/installed_delete_state.py",
                 "scripts/run-installed-delete-disconnect-e2e.sh"):
        path = base.REPO / name
        info["inputs"][str(path)] = startup.digest(path)
    return info


def immutable_scope(operation):
    """Pin the full operation/plan, omitting only declared mutable progress."""
    result = copy.deepcopy(operation)
    for key in ("status", "updated_at", "completed_at"):
        result.pop(key, None)
    for step in result["machine_steps"] + result["cleanup_steps"]:
        for key in ("status", "failure_reason", "resulting_incarnation", "resulting_activation"):
            step.pop(key, None)
    return result


def validate_operation(operation, environment, bindings, request, selector):
    require(operation["schema_version"] == 1 and operation["kind"] == "delete" and
            operation["project_id"] == environment["project_id"] and
            operation["environment_id"] == environment["environment_id"] and
            operation["definition_digest"] == environment["definition_digest"] and
            operation["request_id"] == operation["idempotency_key"] == request and
            operation["generation"] == environment["lifecycle_generation"] + 1 and
            operation["initial_state"] == environment["state"] and operation["requested_target"] == "deleted" and
            operation["request_hash"] == base.request_hash(environment["project_id"], environment["environment_id"], selector),
            "Delete operation changed selected immutable scope")
    wanted = {row["machine"]["machine_id"]: row for row in bindings}
    steps = operation["machine_steps"]
    require(len(steps) == len(wanted) and {row["machine_id"] for row in steps} == set(wanted), "Delete Machine set changed")
    states = {row["machine_id"]: row["state"] for row in environment["machines"]}
    for step in steps:
        require(step["initial_state"] == states[step["machine_id"]] and
                step["expected_incarnation"] == wanted[step["machine_id"]]["incarnation"] and
                step.get("target_state") is None and step.get("resulting_incarnation") is None and
                step.get("resulting_activation") is None, "Delete Machine action/incarnation changed")
    expected = [row for binding in bindings for row in binding["ownership"]]
    actual = [row["ownership"] for row in operation["cleanup_steps"]]
    require(sorted(actual, key=base.ownership_key) == sorted(expected, key=base.ownership_key),
            "Delete ownership cleanup graph changed")


def validate_admission(rows, environment, bindings, request, selector):
    require(len(rows) >= 2 and rows[0] == {"schema_version": 1, "record_type": "request_started",
            "operation": "delete_environment", "request_id": request, "idempotency_key": request},
            "Delete omitted exact request-start record")
    previous, scope = -1, None
    for row in rows[1:]:
        require(row["schema_version"] == 1 and row["record_type"] == "operation_progress" and
                row["request_id"] == row["idempotency_key"] == request and row["terminal"] is False and
                row.get("error") is None and row.get("tombstone") is None and
                type(row["sequence"]) is int and row["sequence"] > previous,
                "terminal/error/uncorrelated observer frame cannot prove disconnect")
        previous = row["sequence"]
        operation = row["operation"]
        validate_operation(operation, environment, bindings, request, selector)
        require(operation["status"] == "running" and operation.get("completed_at") is None,
                "observer did not see admitted Running Delete")
        current = immutable_scope(operation)
        require(scope is None or scope == current, "observer immutable plan drifted")
        scope = current
    return rows[-1]["operation"]


def validate_disconnect(before, after, admitted, exit_code, sent_signal, reaped_ns):
    require(sent_signal == signal.SIGTERM and exit_code == -signal.SIGTERM,
            "exact owned observer SIGTERM exit not observed")
    require(type(reaped_ns) is int and all(type(row[key]) is int for row in (before, after)
            for key in ("started_unix_ns", "observed_unix_ns")) and
            before["started_unix_ns"] <= before["observed_unix_ns"] < reaped_ns <
            after["started_unix_ns"] <= after["observed_unix_ns"],
            "durable snapshot was not acquired after observer reap")
    require(before["database_identity"] == after["database_identity"], "Delete evidence database identity changed")
    for snapshot in (before, after):
        validate_running(snapshot, admitted)
    return {"before_signal": before, "after_observer_reap": after, "observer_exit_code": exit_code,
            "signal": int(sent_signal), "observer_reaped_unix_ns": reaped_ns}


def validate_snapshot_source(snapshot):
    require(snapshot["source"] == "read_only_live_wal_transaction_no_lifecycle_dispatch" and
            set(snapshot["database_identity"]) == {"device", "inode"} and
            all(type(value) is int and value > 0 for value in snapshot["database_identity"].values()) and
            type(snapshot["started_unix_ns"]) is int and type(snapshot["observed_unix_ns"]) is int and
            0 < snapshot["started_unix_ns"] <= snapshot["observed_unix_ns"],
            "fresh live-WAL snapshot provenance required")


def validate_running(snapshot, admitted):
    validate_snapshot_source(snapshot)
    expected = sorted(step["machine_id"] for step in admitted["machine_steps"])
    operation, environment = snapshot["operation"], snapshot["environment"]
    require(operation is not None and immutable_scope(operation) == immutable_scope(admitted) and
            operation["status"] == "running" and operation.get("completed_at") is None and
            snapshot["tombstone"] is None and snapshot["environment_present"] is True and
            sorted(snapshot["machine_ids"]) == expected and isinstance(environment, dict) and
            all(environment[key] == admitted[key] for key in ("project_id", "environment_id", "definition_digest")) and
            environment["active_operation_id"] == admitted["operation_id"] and
            environment["lifecycle_generation"] == admitted["generation"] and
            environment["state"] == "deleting" and
            all(step["status"] in ("pending", "running", "succeeded") and step.get("failure_reason") is None and
                step.get("resulting_incarnation") is None and step.get("resulting_activation") is None
                for step in operation["machine_steps"] + operation["cleanup_steps"]),
            "Running window missed or durable authority changed; no retry/replay is allowed")


def validate_completion(snapshot, admitted, environment, bindings, request, selector):
    validate_snapshot_source(snapshot)
    operation, tombstone = snapshot["operation"], snapshot["tombstone"]
    validate_operation(operation, environment, bindings, request, selector)
    require(immutable_scope(operation) == immutable_scope(admitted) and operation["status"] == "succeeded" and
            all(step["status"] == "succeeded" and step.get("failure_reason") is None
                for step in operation["machine_steps"] + operation["cleanup_steps"]) and
            snapshot["environment_present"] is False and snapshot["environment"] is None and snapshot["machine_ids"] == [],
            "Delete completion lacks exact quiescence/cleanup or aggregate absence")
    expected = [row for binding in bindings for row in binding["ownership"]]
    require(tombstone is not None and tombstone["schema_version"] == 1 and
            all(tombstone[key] == environment[key] for key in ("project_id", "environment_id", "definition_digest", "name")) and
            tombstone["delete_operation_id"] == operation["operation_id"] and
            tombstone["lifecycle_generation"] == operation["generation"] and
            type(operation["completed_at"]) is int and tombstone["deleted_at"] == operation["completed_at"] and
            tombstone["ownership_digest"] == base.ownership_digest(expected), "durable tombstone changed exact cleanup scope")
    return {"operation": operation, "tombstone": tombstone, "request_id": request}


class DisconnectHarness(base.DeleteHarness):
    def __init__(self, info):
        super().__init__(info)
        self.observer_process = None

    def disconnected_delete(self, project, selector, environment, bindings):
        request = "delete-disconnect-" + uuid.uuid4().hex
        self.unresolved_deletes.add(request)
        directory = startup.private(self.evidence / "delete-disconnect")
        argv = list(map(str, [self.cli, "--json", "delete", "--environment", selector, "--timeout", "120",
                             "--request-id", request, "--idempotency-key", request]))
        intent = {"schema_version": 1, "argv": argv, "cwd": str(project), "request_id": request,
                  "idempotency_key": request, "executable_sha256": startup.digest(self.cli),
                  "termination_scope": "exact_unreaped_child_pid_only", "test_case_retries": 0,
                  "started_unix_ns": time.time_ns()}
        startup.document(directory / "observer.intent.json", intent)
        reader = DeleteStateReader(self.database, environment, request, selector)
        output, errors, open_pipes = bytearray(), bytearray(), {}
        process, sent, reaped_ns, failure = None, False, None, None

        def drain(wait=0.01):
            if not open_pipes:
                return
            available, _, _ = select.select(list(open_pipes), [], [], wait)
            for descriptor in available:
                target = open_pipes[descriptor]
                block = os.read(descriptor, min(65536, startup.LIMIT + 1 - len(target)))
                if not block:
                    del open_pipes[descriptor]
                else:
                    target.extend(block)
                    require(len(target) <= startup.LIMIT, "bounded observer stream exceeded")

        def rows():
            return [json.loads(line) for line in bytes(output).split(b"\n")[:-1]]

        try:
            process = subprocess.Popen(argv, cwd=project, env=self.env, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            self.observer_process = process
            for pipe, target in ((process.stdout, output), (process.stderr, errors)):
                os.set_blocking(pipe.fileno(), False)
                open_pipes[pipe.fileno()] = target
            raw, stderr, code = self.command("delete-observer-identity", ["/bin/ps", "-p", str(process.pid),
                "-o", "pid=", "-o", "ppid=", "-o", "uid=", "-o", "lstart=", "-o", "command="], success=False)
            fingerprint = raw.decode().strip()
            require(code == 0 and not stderr and fingerprint.split()[:3] ==
                    [str(process.pid), str(os.getpid()), str(os.geteuid())] and " ".join(argv) in fingerprint,
                    "Delete observer exact parent/UID/argv identity missing")
            startup.document(directory / "observer.identity.json", {"pid": process.pid, "parent_pid": os.getpid(),
                "fingerprint": fingerprint, "executable_sha256": intent["executable_sha256"]})
            deadline = time.monotonic() + 30
            while True:
                drain()
                require(process.poll() is None, "observer exited before controlled disconnect")
                require(not errors, "Delete observer emitted stderr before disconnect")
                captured = rows()
                if len(captured) >= 2:
                    admitted = validate_admission(captured, environment, bindings, request, selector)
                    break
                require(time.monotonic() < deadline, "Delete admission observation deadline exceeded")
            before = reader.snapshot()
            startup.document(directory / "before-signal.json", before)
            drain(0)
            validate_admission(rows(), environment, bindings, request, selector)
            require(process.poll() is None, "observer exited before owned signal")
            startup.document(directory / "observer.signal-intent.json", {"pid": process.pid, "signal": int(signal.SIGTERM),
                "operation_id": admitted["operation_id"], "requested_unix_ns": time.time_ns()})
            # Popen is the sole reaper. An unreaped child PID cannot be recycled;
            # never signal its process group (which can contain a daemon).
            os.kill(process.pid, signal.SIGTERM)
            sent = True
            code = process.wait(timeout=5)
            reaped_ns = time.time_ns()
            after = reader.snapshot()
            startup.document(directory / "after-reap.json", after)
            timing = validate_disconnect(before, after, admitted, code, signal.SIGTERM, reaped_ns)
            capture_deadline = time.monotonic() + 2
            while open_pipes and time.monotonic() < capture_deadline:
                drain()
            require(not open_pipes and not errors, "observer streams incomplete or stderr reported after disconnect")
            # Interrupted writes may leave a partial final JSON line. Every
            # complete observed frame must remain correlated and nonterminal.
            validate_admission(rows(), environment, bindings, request, selector)
            deadline = time.monotonic() + 150
            snapshots = 0
            while True:
                snapshot = reader.snapshot()
                startup.document(directory / f"completion-observation-{snapshots:04}.json", snapshot)
                snapshots += 1
                require(snapshot["database_identity"] == after["database_identity"] and
                        snapshot["started_unix_ns"] > after["observed_unix_ns"], "completion snapshot source changed")
                operation = snapshot["operation"]
                require(operation is not None and immutable_scope(operation) == immutable_scope(admitted),
                        "durable Delete disappeared or changed after disconnect")
                if operation["status"] == "succeeded":
                    completed = validate_completion(snapshot, admitted, environment, bindings, request, selector)
                    break
                validate_running(snapshot, admitted)
                require(time.monotonic() < deadline, "Delete retained supervisor completion deadline exceeded")
                time.sleep(0.1)
            # No public lifecycle call has occurred between observer disconnect
            # and this fsynced positive durable completion record.
            completion_path = directory / "completed-without-replay.json"
            startup.document(completion_path, {"snapshot": snapshot, "result": completed,
                "scope": "read_only_durable_completion_before_any_replay_dispatch", "replay_requests_sent": 0})
            self.verify_deleted(bindings, completed)
            replay_started = time.time_ns()
            raw, stderr, code = self.command("public-delete-exact-replay", argv, cwd=project, timeout=60, success=False)
            require(code == 0 and not stderr, "completed Delete replay failed")
            replay = base.delete_terminal(raw, environment, bindings, request, selector)
            require(replay == completed, "replay changed already durable completion")
            self.unresolved_deletes.remove(request)
            self.cleanup_targets.remove((project, environment["environment_id"]))
            self.deleted.append(completed)
            return {"request_id": request, "timing": timing, "completion": completed, "replay": replay,
                "completion_before_replay_sha256": startup.digest(completion_path),
                "replay_started_unix_ns": replay_started, "read_only_completion_observations": snapshots}
        except BaseException as error:
            failure = f"{type(error).__name__}: {error}"
            raise
        finally:
            if process is not None:
                if process.poll() is None and not sent:
                    startup.document(directory / "observer.failure-term-intent.json", {"pid": process.pid,
                        "signal": int(signal.SIGTERM), "requested_unix_ns": time.time_ns(),
                        "scope": "owned_unreaped_observer_only_not_daemon_or_runtime_cleanup"})
                    os.kill(process.pid, signal.SIGTERM)
                    sent = True
                if process.poll() is None:
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        failure = (failure or "") + "; owned CLI observer remains unreaped; no signal escalation"
                deadline = time.monotonic() + 2
                try:
                    while open_pipes and time.monotonic() < deadline:
                        drain()
                except BaseException as error:
                    failure = (failure or "") + f"; output capture: {error}"
                for pipe in (process.stdout, process.stderr):
                    pipe.close()
            reader.close()
            startup.write(directory / "observer.stdout", bytes(output))
            startup.write(directory / "observer.stderr", bytes(errors))
            startup.document(directory / "observer.result.json", {"pid": None if process is None else process.pid,
                "exit_code": None if process is None else process.returncode, "signal_sent": sent,
                "reaped_unix_ns": reaped_ns, "error": failure, "capture_complete": not open_pipes,
                "stdout_sha256": hashlib.sha256(output).hexdigest(), "stderr_sha256": hashlib.sha256(errors).hexdigest()})

    def scenario(self):
        self.public_inventory()
        project = self.project("developer-delete-disconnect", "developer", 2)
        startup.write(project / "user-sentinel.txt", ("host-user-data-" + uuid.uuid4().hex + "\n").encode())
        primary = self.up(project, "primary")
        self.capture_host_files(project)
        self.daemon_identity = self.daemon_fingerprint()
        primary_contexts, primary_bindings = self.inspect(primary), self.bind_environment(primary)
        neighbor = self.up(project, "neighbor")
        startup.exact_developer_topology(primary, neighbor)
        neighbor_contexts, neighbor_bindings = self.inspect(neighbor), self.bind_environment(neighbor)
        for field in ("name", "endpoint", "engine_id"):
            require(len({row[field] for row in primary_contexts + neighbor_contexts}) == 4, "Machines share " + field)
        workloads = [self.sentinel(context) for context in primary_contexts]
        sentinels = [self.sentinel(context) for context in neighbor_contexts]
        self.monitor = base.NeighborMonitor(self, sentinels)
        self.monitor.start()
        before = self.observe_neighbors()
        result = self.disconnected_delete(project, "primary", primary, primary_bindings)
        after = self.observe_neighbors()
        require(self.status(project, neighbor["environment_id"]) == neighbor and self.inspect(neighbor) == neighbor_contexts,
                "neighbor topology changed across disconnected Delete")
        self.monitor.finish()
        samples = {"samples": self.monitor.samples, "errors": self.monitor.errors,
                   "before": before, "after": after, "scope": "sampled_liveness_not_uninterrupted_availability"}
        self.monitor = None
        neighbor_delete = self.delete(project, "neighbor", neighbor, neighbor_bindings)
        self.check_defaults()
        return {"primary": primary, "primary_workloads": workloads, "neighbor": neighbor, "neighbor_sentinels": sentinels,
                "disconnected_delete": result, "neighbor_delete": neighbor_delete, "sampled_neighbor_liveness": samples}

    def cleanup(self):
        require(self.observer_process is None or self.observer_process.poll() is not None,
                "owned Delete observer unreaped; lifecycle cleanup withheld")
        return super().cleanup()


def run(info):
    os.umask(0o077)
    harness = DisconnectHarness(info)
    startup.document(harness.evidence / "inputs.json", info)
    outcome = {"schema_version": 1, "scope": SCOPE, "outcome": "failed", "error": None, "cleanup_errors": [],
        "docker_parity_certified": False, "aggregate_release_certified": False, "test_case_retries": 0,
        "retained_root": str(harness.root), "failure_policy": "no_retry_preserve_uncertain_Delete_and_original_runtime"}
    try:
        harness.stage()
        outcome["scenario"] = harness.scenario()
        for path, expected in (info["inputs"] | harness.staged_inputs).items():
            require(startup.digest(Path(path)) == expected, "selected input changed during physical run")
    except BaseException as error:
        outcome["error"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            outcome["cleanup"] = harness.cleanup()
        except BaseException as error:
            outcome["cleanup_errors"].append(f"{type(error).__name__}: {error}")
        try:
            startup.collect_runtime_receipts(harness)
        except BaseException as error:
            outcome["cleanup_errors"].append(f"runtime evidence: {type(error).__name__}: {error}")
        if outcome["error"] is None and not outcome["cleanup_errors"]:
            if outcome["cleanup"]["positive_delete_count"] == 2 and outcome["cleanup"]["daemon_graceful_shutdown_observed"]:
                outcome["outcome"] = "passed_dev_installed_public_delete_observer_disconnect"
            else:
                outcome["cleanup_errors"].append("disconnect proof lacks two completed Deletes or graceful daemon shutdown")
        outcome["unresolved_delete_requests"] = sorted(harness.unresolved_deletes)
        outcome["unresolved_up_requests"] = sorted(harness.unresolved_up)
        startup.document(harness.evidence / "result.json", outcome)
        startup.checksum_evidence(harness)
    print(json.dumps(outcome), flush=True)
    return 0 if outcome["outcome"].startswith("passed_") else 1


def main(argv):
    try:
        return run(preflight(arguments(argv)))
    except (Exception, KeyboardInterrupt) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
