"""Independent raw quiescence replay for the fresh, same-daemon Delete DEV gate.

Only Drained is accepted: Ready must physically stop its original runtime;
Stopped must rebind its positively completed Stop (AlreadyAbsent). Persisted
absence/ack recovery needs other evidence and is deliberately out of scope.
No filesystem access, dispatch, retries, or cleanup occurs here.
"""
from __future__ import annotations

import copy
import json
import re


def require(condition, message):
    if not condition:
        raise ValueError("Delete quiescence: " + message)


def shape(value, required, optional=()):
    require(type(value) is dict and set(required) <= set(value) <= set(required) | set(optional),
            "missing or unknown object fields")


def integer(value, minimum=0):
    require(type(value) is int and minimum <= value <= 2**64 - 1, "invalid unsigned integer")


def exact(left, right):
    # Unlike Python equality, canonical JSON does not equate true with 1.
    return json.dumps(left, sort_keys=True, separators=(",", ":"), allow_nan=False) == json.dumps(
        right, sort_keys=True, separators=(",", ":"), allow_nan=False)


def operation_scope(operation, terminal):
    required = {"schema_version", "operation_id", "project_id", "environment_id", "kind", "generation",
                "request_id", "idempotency_key", "request_hash", "definition_digest", "initial_state",
                "requested_target", "status", "machine_steps", "cleanup_steps", "created_at", "updated_at"}
    shape(operation, required, {"completed_at"})
    integer(operation["schema_version"], 1)
    integer(operation["generation"], 1)
    require(operation["schema_version"] == 1 and operation["kind"] == "delete" and
            operation["initial_state"] in ("ready", "stopped") and operation["requested_target"] == "deleted",
            "unsupported lifecycle scope")
    for key in ("operation_id", "project_id", "environment_id", "request_id", "idempotency_key"):
        value = operation[key]
        require(type(value) is str and 0 < len(value.encode()) <= 256 and value.strip() == value and
                all(ord(character) >= 32 and ord(character) != 127 for character in value), "invalid operation identity")
    for key in ("request_hash", "definition_digest"):
        require(type(operation[key]) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", operation[key]), "invalid digest")
    for key in ("created_at", "updated_at"):
        integer(operation[key], 1)
    require(operation["updated_at"] >= operation["created_at"], "nonmonotonic operation timestamps")
    if terminal:
        require(operation["status"] == "succeeded", "final operation did not succeed")
        integer(operation.get("completed_at"), 1)
        require(operation["completed_at"] >= operation["updated_at"], "invalid completion timestamp")
    else:
        require(operation["status"] == "running" and operation.get("completed_at") is None,
                "quiescence must precede completed Delete")
    scope = copy.deepcopy(operation)
    for key in ("status", "updated_at", "completed_at"):
        scope.pop(key, None)
    steps = operation["machine_steps"]
    require(type(steps) is list and 1 <= len(steps) <= 128, "invalid Machine inventory")
    ids = []
    for index, step in enumerate(steps):
        shape(step, {"machine_id", "initial_state", "expected_incarnation", "status"},
              {"target_state", "resulting_incarnation", "resulting_activation", "failure_reason"})
        require(type(step["machine_id"]) is str and step["machine_id"], "invalid Machine ID")
        ids.append(step["machine_id"])
        require(step["initial_state"] == operation["initial_state"] and step.get("target_state") is None and
                step.get("resulting_incarnation") is None and step.get("resulting_activation") is None and
                step.get("failure_reason") is None, "Machine action or incarnation changed")
        require(step["status"] in (("succeeded",) if terminal else ("pending", "running", "succeeded")),
                "unexpected Machine progress")
        scope["machine_steps"][index] = {key: step[key] for key in
                                         ("machine_id", "initial_state", "expected_incarnation")}
    require(ids == sorted(set(ids)), "duplicate or unordered Machine inventory")
    cleanup = operation["cleanup_steps"]
    require(type(cleanup) is list and 1 <= len(cleanup) <= 4096, "invalid cleanup inventory")
    keys = []
    for index, step in enumerate(cleanup):
        shape(step, {"ownership", "status"}, {"failure_reason"})
        require(step.get("failure_reason") is None and
                step["status"] in (("succeeded",) if terminal else ("pending", "running", "succeeded")),
                "unexpected cleanup progress")
        row = step["ownership"]
        shape(row, {"schema_version", "resource_kind", "resource_id", "environment_id", "machine_id"})
        integer(row["schema_version"], 1)
        require(row["schema_version"] == 1 and row["environment_id"] == operation["environment_id"] and
                row["machine_id"] in ids and type(row["resource_id"]) is str and row["resource_id"],
                "foreign cleanup ownership")
        kind = row["resource_kind"]
        if type(kind) is dict:
            shape(kind, {"other"})
            require(kind["other"] in ("machine_runtime_store", "runtime_vm"), "unsupported cleanup resource")
            kind = "other:" + kind["other"]
        require(kind in ("machine", "incarnation", "docker_context", "other:machine_runtime_store", "other:runtime_vm"),
                "unsupported cleanup kind")
        keys.append((kind, row["resource_id"], row["machine_id"]))
        scope["cleanup_steps"][index] = {"ownership": copy.deepcopy(row)}
    require(keys == sorted(set(keys)), "duplicate or unordered cleanup inventory")
    return scope


def validate(binding, final_operation, quiescence):
    """Return a copy of the exact validated Drained proof, or raise ValueError.

    `binding` is the driver's pre-Delete public activation/store capture;
    `final_operation` comes from its validated public terminal Delete stream;
    `quiescence` is the raw outside-store intent's retained proof.
    """
    try:
        shape(quiescence, {"schema_version", "owner", "configuration_digest", "operation", "authority"})
        integer(quiescence["schema_version"], 1)
        require(quiescence["schema_version"] == 1 and exact(quiescence["owner"], binding["owner"]) and
                quiescence["configuration_digest"] == binding["manifest"]["configuration_digest"],
                "schema, owner or configuration differs from original store")
        final_scope = operation_scope(final_operation, True)
        prior = quiescence["operation"]
        require(exact(operation_scope(prior, False), final_scope), "immutable Delete operation or ownership plan changed")
        require(prior["updated_at"] <= final_operation["completed_at"], "quiescence dated after final deletion")
        owner = binding["owner"]
        require(owner["project_id"] == final_operation["project_id"] and
                owner["environment_id"] == final_operation["environment_id"] and
                owner["machine_id"] == binding["machine"]["machine_id"], "binding belongs to another operation")
        selected = [step for step in final_operation["machine_steps"] if step["machine_id"] == owner["machine_id"]]
        require(len(selected) == 1 and exact(selected[0]["expected_incarnation"], binding["incarnation"]),
                "original public Machine incarnation changed")
        require(all(any(exact(row, step["ownership"]) for step in final_operation["cleanup_steps"])
                    for row in binding["ownership"]), "original store ownership missing from Delete")
        authority = quiescence["authority"]
        shape(authority, {"kind", "runtime_identity", "endpoint", "outcome"})
        require(authority["kind"] == "drained", "fresh same-daemon gate requires original Drained authority")
        public_runtime = binding["runtime_identity"]
        shape(public_runtime, {"schema_version", "opaque_id"})
        integer(public_runtime["schema_version"], 1)
        require(public_runtime["schema_version"] == 1, "unsupported public runtime identity")
        runtime = json.loads(public_runtime["opaque_id"])
        shape(runtime, {"schema_version", "stack_id", "incarnation_id"})
        integer(runtime["schema_version"], 1)
        require(runtime["schema_version"] == 1 and exact(authority["runtime_identity"], runtime),
                "original backend runtime identity changed")
        vm = [row for row in binding["ownership"] if row["resource_kind"] == {"other": "runtime_vm"}]
        require(len(vm) == 1 and vm[0]["resource_id"] == runtime["stack_id"], "runtime does not name exact owned VM")
        expected_outcome = "stopped" if selected[0]["initial_state"] == "ready" else "already_absent"
        require(authority["outcome"] == expected_outcome, "missing expected positive original-runtime shutdown outcome")
        endpoint = authority["endpoint"]
        shape(endpoint, {"accepted_connections", "completed_connections", "cancelled_connections",
                         "failed_connections", "active_connections", "socket_removed"})
        for key in set(endpoint) - {"socket_removed"}:
            integer(endpoint[key])
        require(endpoint["socket_removed"] is True and endpoint["active_connections"] == 0 and
                endpoint["accepted_connections"] == endpoint["completed_connections"] +
                endpoint["cancelled_connections"] + endpoint["failed_connections"],
                "endpoint retains socket, active handlers or unaccounted connections")
        return copy.deepcopy(quiescence)
    except (KeyError, TypeError, json.JSONDecodeError, OverflowError) as error:
        raise ValueError("Delete quiescence: malformed proof") from error
