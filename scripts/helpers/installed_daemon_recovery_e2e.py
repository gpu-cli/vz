#!/usr/bin/env python3
"""DEV installed dead-daemon recovery after positive Stop, not live adoption.

No harness socket deletion, database writes, daemon spawn fallback, or retries.
The production recovery receipt contract must bind each stale socket action.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
import time
import uuid

import installed_delete_e2e as base
import installed_delete_quiescence as quiescence
import installed_developer_startup as startup

require = startup.require
SCOPE = "DEV_INSTALLED_DEAD_DAEMON_RECOVERY_AFTER_POSITIVE_STOP_NOT_RELEASE_CERTIFICATION"
CONTROL_SCOPE = "control_socket_only_not_VM_quiescence"


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
    for name in ("scripts/helpers/installed_daemon_recovery_e2e.py", "scripts/run-installed-daemon-recovery-e2e.sh"):
        path = base.REPO / name
        info["inputs"][str(path)] = startup.digest(path)
    return info


def path_identity(path, kind):
    require(path.resolve(strict=True) == path, "redirected daemon authority path")
    value = path.lstat()
    predicate = {"socket": stat.S_ISSOCK, "file": stat.S_ISREG, "directory": stat.S_ISDIR}[kind]
    require(predicate(value.st_mode) and value.st_uid == os.geteuid() and value.st_mode & 0o077 == 0,
            "daemon authority path lacks exact private owned type")
    if kind == "file":
        require(value.st_nlink == 1, "multiply linked daemon authority file")
    return {"device": value.st_dev, "inode": value.st_ino}


def validate_process_identity(value):
    quiescence.shape(value, {"pid", "uid", "start_seconds", "start_microseconds", "boot_session_uuid"})
    require(type(value["pid"]) is int and 1 < value["pid"] <= 2**31 - 1 and
            type(value["uid"]) is int and 0 <= value["uid"] <= 2**32 - 1 and
            type(value["start_seconds"]) is int and value["start_seconds"] > 0 and
            type(value["start_microseconds"]) is int and 0 <= value["start_microseconds"] < 1000000 and
            isinstance(value["boot_session_uuid"], str) and
            re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", value["boot_session_uuid"]),
            "invalid native daemon process birth identity")


def stop_hash(environment, selector):
    encoded = json.dumps([environment["project_id"], environment["environment_id"],
        {"explicit": {"kind": "name_or_id", "value": selector}}, 120000], separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def stop_terminal(raw, environment, bindings, request, selector):
    """Authenticate the complete positive public Stop before authorizing crash."""
    rows = [json.loads(line) for line in raw.splitlines()]
    require(len(rows) >= 2 and rows[0] == {"schema_version": 1, "record_type": "request_started",
        "operation": "stop_environment", "request_id": request, "idempotency_key": request}, "missing exact Stop request preamble")
    events = rows[1:]
    require(sum(row.get("terminal") is True for row in events) == 1 and events[-1]["terminal"] is True,
            "exact single final Stop terminal required")
    expected = {row["machine"]["machine_id"]: row for row in bindings}
    states = {row["machine_id"]: row["state"] for row in environment["machines"]}
    scope, sequence = None, -1
    for event in events:
        require(event["schema_version"] == 1 and event["record_type"] == "operation_progress" and
                event["request_id"] == event["idempotency_key"] == request and event.get("error") is None and
                type(event["sequence"]) is int and event["sequence"] > sequence, "Stop progress is uncorrelated/error/nonmonotonic")
        sequence = event["sequence"]
        operation = event["operation"]
        require(operation["schema_version"] == 1 and operation["kind"] == "stop" and
                all(operation[key] == environment[key] for key in ("project_id", "environment_id", "definition_digest")) and
                operation["generation"] == environment["lifecycle_generation"] + 1 and
                operation["request_id"] == operation["idempotency_key"] == request and
                operation["request_hash"] == stop_hash(environment, selector) and
                operation["initial_state"] == environment["state"] and operation["requested_target"] == "stopped" and
                operation["cleanup_steps"] == [], "Stop changed exact Environment action/scope")
        steps = operation["machine_steps"]
        require(len(steps) == len(expected) and {step["machine_id"] for step in steps} == set(expected), "Stop Machine inventory changed")
        for step in steps:
            require(step["initial_state"] == states[step["machine_id"]] and step["target_state"] == "stopped" and
                    quiescence.exact(step["expected_incarnation"], expected[step["machine_id"]]["incarnation"]) and
                    step.get("resulting_incarnation") is None and step.get("resulting_activation") is None and
                    step.get("failure_reason") is None and step["status"] in ("pending", "running", "succeeded"),
                    "Stop Machine action, incarnation or positive progress changed")
        immutable = copy.deepcopy(operation)
        for key in ("status", "updated_at", "completed_at"):
            immutable.pop(key, None)
        for step in immutable["machine_steps"]:
            step.pop("status", None)
        require(scope is None or quiescence.exact(scope, immutable), "Stop immutable operation drifted")
        scope = immutable
        require(operation["status"] == ("succeeded" if event["terminal"] else "running"), "Stop terminal/state mismatch")
    terminal = events[-1]["operation"]
    require(type(terminal["completed_at"]) is int and terminal["created_at"] <= terminal["updated_at"] <= terminal["completed_at"] and
            all(step["status"] == "succeeded" for step in terminal["machine_steps"]), "Stop lacks complete positive Machine acknowledgements")
    return terminal


def validate_prior_stop_quiescence(binding, final_operation, proof, prior_stop):
    """Only the exact observed precrash positive Stop can authorize absence.

    This is distinct from a physical Drained receipt. No endpoint counters or
    live Runtime identity are invented for the newly started daemon.
    """
    quiescence.shape(proof, {"schema_version", "owner", "configuration_digest", "operation", "authority"})
    require(type(proof["schema_version"]) is int and proof["schema_version"] == 1 and
            quiescence.exact(proof["owner"], binding["owner"]) and
            proof["configuration_digest"] == binding["manifest"]["configuration_digest"], "restart Delete proof owner/configuration mismatch")
    require(quiescence.exact(quiescence.operation_scope(proof["operation"], False),
                            quiescence.operation_scope(final_operation, True)), "restart Delete immutable plan changed")
    require(proof["operation"]["updated_at"] <= final_operation["completed_at"], "quiescence proof is dated after Delete completion")
    authority = proof["authority"]
    quiescence.shape(authority, {"kind", "authority"})
    require(authority["kind"] == "absent", "restarted daemon must use explicit positive Stop absence authority")
    inner = authority["authority"]
    quiescence.shape(inner, {"kind", "operation"})
    require(inner["kind"] == "positive_stop" and quiescence.exact(inner["operation"], prior_stop),
            "absence proof is not the exact precrash observed positive Stop")
    require(prior_stop["kind"] == "stop" and prior_stop["status"] == "succeeded" and
            all(prior_stop[key] == final_operation[key] for key in ("project_id", "environment_id", "definition_digest")) and
            prior_stop["generation"] + 1 == final_operation["generation"] and
            final_operation["initial_state"] == "stopped" and prior_stop["requested_target"] == "stopped" and
            prior_stop["completed_at"] <= final_operation["created_at"], "Delete is not the immediate exact stopped successor")
    machine_id = binding["owner"]["machine_id"]
    before = [row for row in prior_stop["machine_steps"] if row["machine_id"] == machine_id]
    after = [row for row in final_operation["machine_steps"] if row["machine_id"] == machine_id]
    require(len(before) == len(after) == 1 and before[0]["status"] == "succeeded" and
            before[0]["target_state"] == "stopped" and after[0]["initial_state"] == "stopped" and
            quiescence.exact(before[0]["expected_incarnation"], binding["incarnation"]) and
            quiescence.exact(after[0]["expected_incarnation"], binding["incarnation"]), "positive Stop changed original Machine incarnation")
    return copy.deepcopy(proof)


def validate_recovery_record(previous, replacement, receipt):
    quiescence.shape(receipt, {"schema_version", "daemon_id", "previous_daemon_id", "previous_owner_sha256",
                              "previous_process_observation", "graceful_closed", "scope"})
    old, new = previous["record"], replacement["record"]
    validate_process_identity(old["process"])
    validate_process_identity(new["process"])
    require(old["process"]["boot_session_uuid"] == new["process"]["boot_session_uuid"],
            "this uninterrupted host fixture does not certify cross-boot recovery")
    require(type(receipt["schema_version"]) is int and receipt["schema_version"] == 1 and
            receipt["daemon_id"] == new["daemon_id"] and receipt["previous_daemon_id"] == old["daemon_id"] and
            new["daemon_id"] != old["daemon_id"] and receipt["previous_owner_sha256"] == previous["sha256"] and
            receipt["scope"] == CONTROL_SCOPE and receipt["graceful_closed"] is None,
            "recovery record does not bind exact crashed owner/control-only scope")
    observation = receipt["previous_process_observation"]
    if observation is not None:
        quiescence.shape(observation, {"identity", "zombie"})
        validate_process_identity(observation["identity"])
        require(type(observation["zombie"]) is bool and observation["identity"]["pid"] == old["process"]["pid"] and
                observation["identity"]["boot_session_uuid"] == old["process"]["boot_session_uuid"] and
                (observation["zombie"] or not quiescence.exact(observation["identity"], old["process"])),
                "recovery record still observes the original live daemon birth")
    require(not quiescence.exact(old["process"], new["process"]) and
            all(quiescence.exact(old[key], new[key]) for key in ("configuration", "socket_parent", "state_parent",
                "runtime_root", "history_root", "staging_parent", "database", "database_lock", "socket_lock", "log")) and
            old["socket"]["path"] == new["socket"]["path"] and
            old["socket"]["staging_path"] == new["socket"]["staging_path"] and
            old["socket"]["identity"] != new["socket"]["identity"],
            "recovery replaced persistence/locks/configuration or failed to replace exact dead socket")
    return copy.deepcopy(receipt)


def validate_closed_record(owner, receipt):
    quiescence.shape(receipt, {"schema_version", "daemon_id", "owner_sha256", "socket_removed", "pid_removed", "scope"})
    require(type(receipt["schema_version"]) is int and receipt["schema_version"] == 1 and
            receipt["daemon_id"] == owner["record"]["daemon_id"] and receipt["owner_sha256"] == owner["sha256"] and
            receipt["socket_removed"] is True and receipt["pid_removed"] is True and receipt["scope"] == CONTROL_SCOPE,
            "graceful close does not bind replacement control owner")
    return copy.deepcopy(receipt)


class RecoveryHarness(base.DeleteHarness):
    def __init__(self, info):
        super().__init__(info)
        self.positive_stops = {}
        self.crashed_environments = set()
        self.recovery_pending = False
        self.replacement_owner = None

    def stopped_with_authority(self, project, environment, bindings):
        start = len(self.record.receipts)
        stopped = self.stop(project, environment["environment_id"])
        commands = [row for row in self.record.receipts[start:] if row["label"] == "public-stop"]
        require(len(commands) == 1, "one public Stop dispatch required")
        command = commands[0]
        request = command["argv"][command["argv"].index("--request-id") + 1]
        raw = startup.read_private_regular(self.evidence / f'{command["index"]:03}-public-stop.stdout', startup.LIMIT)
        stderr = startup.read_private_regular(self.evidence / f'{command["index"]:03}-public-stop.stderr', startup.LIMIT)
        require(command["exit_code"] == 0 and command["capture_complete"] is True and command["error"] is None and
                command["effects_uncertain"] is False and not stderr and
                command["stdout_sha256"] == hashlib.sha256(raw).hexdigest() and
                command["stderr_sha256"] == hashlib.sha256(stderr).hexdigest(), "positive Stop lacks complete bounded raw command proof")
        operation = stop_terminal(raw, environment, bindings, request, environment["environment_id"])
        require(stopped["lifecycle_generation"] == operation["generation"] and stopped.get("active_operation_id") is None and
                stopped["environment_id"] == environment["environment_id"] and
                {row["machine_id"] for row in stopped["machines"]} == {row["machine_id"] for row in environment["machines"]},
                "public stopped topology differs from exact Stop operation")
        self.positive_stops[environment["environment_id"]] = operation
        startup.document(self.evidence / (environment["environment_id"] + "-" + operation["operation_id"] + "-positive-stop.json"),
                         {"environment": stopped, "operation": operation, "raw_stdout_sha256": hashlib.sha256(raw).hexdigest()})
        return stopped

    def capture_owner(self, label, fingerprint):
        current = Path(str(self.socket) + ".owner.json")
        raw = startup.read_private_regular(current, startup.LIMIT)
        owner = json.loads(raw)
        quiescence.shape(owner, {"schema_version", "daemon_id", "process", "configuration", "socket_parent", "state_parent",
            "runtime_root", "history_root", "staging_parent", "database", "database_lock", "socket_lock", "socket", "log", "pid", "preparation"})
        validate_process_identity(owner["process"])
        require(type(owner["schema_version"]) is int and owner["schema_version"] == 1 and
                isinstance(owner["daemon_id"], str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", owner["daemon_id"]),
                "invalid exact daemon owner schema/identity")
        require(owner["process"]["pid"] == fingerprint["pid"] and owner["process"]["uid"] == os.geteuid(),
                "daemon owner record differs from live exact process")
        wanted = {"socket_path": str(self.socket), "state_store_path": str(self.database),
                  "runtime_data_dir": str(self.runtime), "log_path": str(self.socket.with_suffix(".log")),
                  "pid_path": str(self.socket.with_suffix(".pid"))}
        require(owner["configuration"] == wanted, "daemon owner adopted another configuration")
        for name, path, kind in (("socket_parent", self.socket.parent, "directory"),
                                 ("state_parent", self.database.parent, "directory"),
                                 ("runtime_root", self.runtime, "directory"), ("database", self.database, "file")):
            require(owner[name] == path_identity(path, kind), "daemon owner changed " + name)
        for name, path, kind in (("socket", self.socket, "socket"),
                                 ("log", self.socket.with_suffix(".log"), "file"),
                                 ("pid", self.socket.with_suffix(".pid"), "file")):
            require(owner[name]["path"] == str(path) and owner[name]["identity"] == path_identity(path, kind),
                    "daemon owner changed " + name + " path/inode")
        for name in ("database_lock", "socket_lock"):
            path = Path(owner[name]["path"])
            require(path.is_relative_to(self.root) and owner[name]["identity"] == path_identity(path, "file"),
                    "daemon ownership lock changed or escaped fixture")
        history = Path(str(self.socket) + ".owners")
        stage = self.socket.parent / (".c" + hashlib.sha256(self.socket.name.encode()).hexdigest()[:8])
        require(owner["history_root"] == path_identity(history, "directory") and
                owner["staging_parent"] == path_identity(stage, "directory") and
                owner["socket"]["staging_path"] == str(stage / "s") and not os.path.lexists(stage / "s"),
                "daemon staging/history authority differs or published staging name remains")
        require(startup.read_private_regular(history / (owner["daemon_id"] + ".owner.json"), startup.LIMIT) == raw,
                "current owner lacks byte-identical immutable ownership record")
        preparation_path = Path(str(self.socket) + ".preparing.json")
        preparation_raw = startup.read_private_regular(preparation_path, startup.LIMIT)
        preparation = json.loads(preparation_raw)
        require(owner["preparation"] == {"identity": path_identity(preparation_path, "file"),
                                        "sha256": hashlib.sha256(preparation_raw).hexdigest()} and
                preparation == {"schema_version": 1, "daemon_id": owner["daemon_id"], "process": owner["process"],
                    "configuration": owner["configuration"], "staging_path": owner["socket"]["staging_path"]},
                "daemon preparation does not bind published owner/configuration")
        startup.write(self.evidence / (label + ".owner.json"), raw)
        startup.write(self.evidence / (label + ".preparation.json"), preparation_raw)
        return {"record": owner, "sha256": hashlib.sha256(raw).hexdigest(),
                "record_path_identity": path_identity(current, "file"), "fingerprint": fingerprint}

    def capture_recovery(self, previous, replacement):
        history = Path(str(self.socket) + ".owners")
        old_path = history / (previous["record"]["daemon_id"] + ".owner.json")
        require(startup.digest(old_path) == previous["sha256"], "immutable previous owner changed after recovery")
        require(not os.path.lexists(history / (previous["record"]["daemon_id"] + ".closed.json")),
                "deliberately crashed daemon unexpectedly has graceful close authority")
        path = history / (replacement["record"]["daemon_id"] + ".recovery.json")
        raw = startup.read_private_regular(path, startup.LIMIT)
        receipt = validate_recovery_record(previous, replacement, json.loads(raw))
        startup.write(self.evidence / "production-daemon-recovery.json", raw)
        require(startup.digest(Path(str(self.socket) + ".owner.json")) == replacement["sha256"],
                "replacement current owner changed during recovery verification")
        return {"record": receipt, "sha256": hashlib.sha256(raw).hexdigest(), "path": str(path),
                "harness_socket_unlinks": 0, "recovery_retries": 0}

    def capture_closed(self, owner):
        path = Path(str(self.socket) + ".owners") / (owner["record"]["daemon_id"] + ".closed.json")
        raw = startup.read_private_regular(path, startup.LIMIT)
        receipt = validate_closed_record(owner, json.loads(raw))
        require(not os.path.lexists(self.socket) and not os.path.lexists(self.socket.with_suffix(".pid")),
                "closed receipt contradicts retained control socket/PID file")
        startup.write(self.evidence / "replacement-daemon-closed.json", raw)
        return {"record": receipt, "sha256": hashlib.sha256(raw).hexdigest()}

    def crash_stopped_daemon(self, stopped, bindings):
        require(len(stopped) == 2 and sum(len(row["machines"]) for row in stopped) == 4 and
                all(row["state"] == "stopped" and row.get("active_operation_id") is None and
                    all(machine["state"] == "stopped" for machine in row["machines"]) for row in stopped) and
                set(self.positive_stops) == {row["environment_id"] for row in stopped},
                "deliberate crash requires four positively stopped Machines")
        for binding in bindings:
            require(not os.path.lexists(binding["descriptor"]["endpoint"][7:]), "Machine endpoint remains before daemon crash")
            require(base.identity(Path(binding["store_path"])) == binding["store_identity"], "stopped store identity changed")
        self.check_defaults()
        first = self.daemon_fingerprint()
        require(first == self.daemon_identity, "original daemon fingerprint changed")
        owner = self.capture_owner("precrash", first)
        second = self.daemon_fingerprint()
        require(second == first, "daemon birth/invocation changed before deliberate crash")
        self.recovery_pending = True
        self.crashed_environments = {row["environment_id"] for row in stopped}
        startup.document(self.evidence / "daemon-crash-intent.json", {"signal": int(signal.SIGKILL),
            "first_fingerprint": first, "second_fingerprint": second, "owner_sha256": owner["sha256"],
            "positive_stop_operation_ids": [self.positive_stops[row["environment_id"]]["operation_id"] for row in stopped],
            "scope": "exact_owned_daemon_pid_only_after_all_Machines_positive_Stop", "started_unix_ns": time.time_ns()})
        os.kill(first["pid"], signal.SIGKILL)
        deadline = time.monotonic() + 30
        while True:
            raw, _, code = self.command("crashed-daemon-absence", ["/bin/ps", "-p", str(first["pid"]), "-o", "pid="], success=False)
            if code != 0 and not raw.strip():
                break
            require(time.monotonic() < deadline, "crashed daemon PID remains; no repeated signal")
            time.sleep(0.1)
        current = Path(str(self.socket) + ".owner.json")
        require(startup.digest(current) == owner["sha256"] and path_identity(current, "file") == owner["record_path_identity"] and
                path_identity(self.socket, "socket") == owner["record"]["socket"]["identity"],
                "stale socket/owner changed before production recovery")
        startup.document(self.evidence / "daemon-crash-observed.json", {"old_pid_absent": True,
            "observed_unix_ns": time.time_ns(), "retained_owner": owner,
            "socket_retained": True, "harness_socket_unlinks": 0})
        return owner

    def verify_deleted(self, bindings, result):
        environment_id = result["operation"]["environment_id"]
        if environment_id not in self.crashed_environments:
            return super().verify_deleted(bindings, result)
        prior = self.positive_stops[environment_id]
        for binding in bindings:
            for path in (binding["store_path"], binding["context_path"], binding["tls_path"], binding["descriptor"]["endpoint"][7:]):
                require(not os.path.lexists(path), "deleted stopped owned path remains: " + path)
            directory = self.runtime / "topology-machine-deletions" / Path(binding["store_path"]).name
            base.identity(directory)
            raw = startup.read_private_regular(directory / "intent.json", startup.LIMIT)
            intent = json.loads(raw)
            receipt = json.loads(startup.read_private_regular(directory / "receipt.json", startup.LIMIT))
            operation = result["operation"]
            validate_prior_stop_quiescence(binding, operation, intent["quiescence"], prior)
            require(intent["schema_version"] == receipt["schema_version"] == 1 and intent["manifest"] == binding["manifest"] and
                    intent["operation"]["operation_id"] == receipt["operation_id"] == operation["operation_id"] and
                    intent["operation"]["generation"] == receipt["generation"] == operation["generation"] and
                    intent["operation"]["request_hash"] == operation["request_hash"] and
                    intent["operation"]["definition_digest"] == operation["definition_digest"] and
                    receipt["owner"] == binding["owner"] and receipt["configuration_digest"] == binding["manifest"]["configuration_digest"] and
                    intent["store"] == receipt["store"] == binding["store_identity"] and
                    intent["data"] == receipt["data"] == binding["data_identity"] and
                    receipt["intent_sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest() and receipt["store_removed"] is True and
                    not os.path.lexists(directory / "store"), "restarted Delete receipt lost exact original store authority")
            _, _, code = self.docker("recovered-deleted-context-rejected", binding["descriptor"], ["info", "--format", "{{.ID}}"],
                                     timeout=15, success=False)
            require(code != 0, "deleted stopped context still reaches an Engine")
        self.check_defaults()

    def restart_sentinel(self, previous, descriptor):
        require(all(previous["descriptor"][key] == descriptor[key] for key in ("owner", "name", "endpoint", "engine_id", "config_dir")),
                "neighbor persistent Engine/context identity changed")
        raw, _, _ = self.docker("persisted-container-inspect", descriptor, ["container", "inspect", previous["container_id"]])
        items = json.loads(raw)
        require(len(items) == 1, "exact persisted container missing")
        item = items[0]
        require(item["Id"] == previous["container_id"] and item["Image"] == previous["image_id"] and
                item["Config"]["Labels"][base.LABEL] == previous["token"] and item["HostConfig"]["Runtime"] == "youki" and
                item["State"]["Running"] is False and
                any(row["Type"] == "volume" and row["Name"] == previous["volume"] and row["Destination"] == "/sentinel" for row in item["Mounts"]),
                "persisted container changed ownership or unexpectedly runs")
        raw, _, _ = self.docker("persisted-volume-inspect", descriptor, ["volume", "inspect", previous["volume"]])
        volumes = json.loads(raw)
        require(len(volumes) == 1 and volumes[0]["Name"] == previous["volume"] and volumes[0]["Labels"][base.LABEL] == previous["token"],
                "persisted volume changed ownership")
        raw, _, _ = self.docker("persisted-container-start", descriptor, ["container", "start", previous["container_id"]])
        require(raw.decode().strip() == previous["container_id"], "started container identity changed")
        raw, _, _ = self.docker("restarted-container-inspect", descriptor, ["container", "inspect", previous["container_id"]])
        items = json.loads(raw)
        require(len(items) == 1 and items[0]["Id"] == previous["container_id"], "restarted container identity changed")
        observed = dict(previous, descriptor=descriptor, started_at=items[0]["State"]["StartedAt"])
        self.check_sentinel(observed)
        return {"before": previous, "after": observed, "scope": "explicit_restart_of_exact_owned_persisted_container_and_volume"}

    def scenario(self):
        self.public_inventory()
        project = self.project("developer-daemon-recovery", "developer", 2)
        startup.write(project / "user-sentinel.txt", ("host-user-data-" + uuid.uuid4().hex + "\n").encode())
        primary = self.up(project, "primary")
        self.capture_host_files(project)
        self.daemon_identity = self.daemon_fingerprint()
        primary_contexts, primary_bindings = self.inspect(primary), self.bind_environment(primary)
        neighbor = self.up(project, "neighbor")
        startup.exact_developer_topology(primary, neighbor)
        neighbor_contexts, neighbor_bindings = self.inspect(neighbor), self.bind_environment(neighbor)
        for key in ("name", "endpoint", "engine_id"):
            require(len({row[key] for row in primary_contexts + neighbor_contexts}) == 4, "Machines share " + key)
        primary_workloads = [self.sentinel(context) for context in primary_contexts]
        sentinels = [self.sentinel(context) for context in neighbor_contexts]
        stopped_primary = self.stopped_with_authority(project, primary, primary_bindings)
        stopped_neighbor = self.stopped_with_authority(project, neighbor, neighbor_bindings)
        previous = self.crash_stopped_daemon([stopped_primary, stopped_neighbor], primary_bindings + neighbor_bindings)
        # This is the first connection attempt after confirmed death. Only the
        # public managed-existing-owner Delete path may recover/spawn a daemon.
        primary_delete = self.delete(project, "primary", stopped_primary, primary_bindings)
        fingerprint = self.daemon_fingerprint()
        require(fingerprint != previous["fingerprint"], "public Delete did not establish a different daemon birth")
        replacement = self.capture_owner("recovered", fingerprint)
        recovery = self.capture_recovery(previous, replacement)
        self.daemon_identity = fingerprint
        self.replacement_owner = replacement
        self.recovery_pending = False
        require(self.status(project, neighbor["environment_id"]) == stopped_neighbor,
                "recovery/Delete changed the stopped neighboring Environment")
        for binding in neighbor_bindings:
            require(base.identity(Path(binding["store_path"])) == binding["store_identity"] and
                    base.identity(Path(binding["store_path"]) / "data") == binding["data_identity"] and
                    not os.path.lexists(binding["descriptor"]["endpoint"][7:]),
                    "recovery touched stopped neighbor storage/endpoint")
        restarted = self.up(project, neighbor["environment_id"])
        require(all(restarted[key] == neighbor[key] for key in ("project_id", "environment_id", "name", "definition_digest")) and
                {row["machine_id"] for row in restarted["machines"]} == {row["machine_id"] for row in neighbor["machines"]},
                "neighbor re-Up changed logical topology identity")
        restarted_contexts = self.inspect(restarted)
        old_contexts = {row["owner"]["machine_id"]: row for row in neighbor_contexts}
        for context in restarted_contexts:
            old = old_contexts[context["owner"]["machine_id"]]
            require(all(context[key] == old[key] for key in ("owner", "name", "endpoint", "engine_id", "config_dir")) and
                    context["incarnation_generation"] > old["incarnation_generation"],
                    "neighbor re-Up changed persistence or failed to advance incarnation")
        # Keep both raw startup generations. The inherited binder intentionally
        # refuses duplicate evidence directories rather than overwriting them.
        original_capture = self.evidence / ("before-" + neighbor["environment_id"])
        retained_capture = self.evidence / ("precrash-before-" + neighbor["environment_id"])
        captured_identity = base.identity(original_capture)
        require(not os.path.lexists(retained_capture), "precrash evidence destination already exists")
        startup.document(self.evidence / "retain-precrash-capture.json", {"source": str(original_capture),
            "destination": str(retained_capture), "identity": captured_identity, "scope": "owned_test_evidence_only"})
        original_capture.rename(retained_capture)
        require(base.identity(retained_capture) == captured_identity, "retained precrash evidence identity changed")
        original_binding = self.evidence / (neighbor["environment_id"] + "-physical-bindings.json")
        retained_binding = self.evidence / ("precrash-" + neighbor["environment_id"] + "-physical-bindings.json")
        binding_identity, binding_digest = path_identity(original_binding, "file"), startup.digest(original_binding)
        require(not os.path.lexists(retained_binding), "precrash binding evidence destination already exists")
        original_binding.rename(retained_binding)
        require(path_identity(retained_binding, "file") == binding_identity and startup.digest(retained_binding) == binding_digest,
                "retained precrash bindings changed")
        restarted_bindings = self.bind_environment(restarted)
        previous_bindings = {row["machine"]["machine_id"]: row for row in neighbor_bindings}
        for binding in restarted_bindings:
            before = previous_bindings[binding["machine"]["machine_id"]]
            require(all(binding[key] == before[key] for key in ("owner", "manifest", "store_path", "store_identity", "data_identity")),
                    "neighbor re-Up replaced pinned persistent store")
        contexts = {row["owner"]["machine_id"]: row for row in restarted_contexts}
        persisted = [self.restart_sentinel(row, contexts[row["descriptor"]["owner"]["machine_id"]]) for row in sentinels]
        self.crashed_environments.remove(neighbor["environment_id"])
        stopped_again = self.stopped_with_authority(project, restarted, restarted_bindings)
        neighbor_delete = self.delete(project, "neighbor", stopped_again, restarted_bindings)
        self.check_defaults()
        return {"primary": primary, "neighbor": neighbor, "primary_workloads": primary_workloads,
                "neighbor_sentinels": sentinels, "stopped_primary": stopped_primary, "stopped_neighbor": stopped_neighbor,
                "precrash_owner": previous, "recovered_owner": replacement, "recovery": recovery,
                "primary_delete": primary_delete, "restarted_neighbor": restarted,
                "persistent_neighbor_workloads": persisted, "neighbor_delete": neighbor_delete}

    def cleanup(self):
        require(not self.recovery_pending, "dead-daemon recovery uncertain; automatic lifecycle/daemon cleanup withheld")
        result = super().cleanup()
        if self.replacement_owner is not None:
            result["replacement_owner_closed"] = self.capture_closed(self.replacement_owner)
        return result


def run(info):
    os.umask(0o077)
    harness = RecoveryHarness(info)
    startup.document(harness.evidence / "inputs.json", info)
    outcome = {"schema_version": 1, "scope": SCOPE, "outcome": "failed", "error": None, "cleanup_errors": [],
        "docker_parity_certified": False, "aggregate_release_certified": False, "live_machine_crash_adoption_certified": False,
        "test_case_retries": 0, "retained_root": str(harness.root),
        "failure_policy": "no_retry_no_harness_socket_unlink_preserve_uncertain_recovery_or_lifecycle"}
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
            if (outcome["cleanup"]["positive_delete_count"] == 2 and outcome["cleanup"]["daemon_graceful_shutdown_observed"] and
                    outcome["cleanup"].get("replacement_owner_closed")):
                outcome["outcome"] = "passed_dev_installed_dead_daemon_recovery_after_positive_stop"
            else:
                outcome["cleanup_errors"].append("recovery proof lacks two completed Deletes or exact graceful replacement close")
        outcome["unresolved_delete_requests"] = sorted(harness.unresolved_deletes)
        outcome["unresolved_up_requests"] = sorted(harness.unresolved_up)
        outcome["recovery_pending"] = harness.recovery_pending
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
