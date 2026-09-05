"""Read-only live-WAL evidence for an exact installed public Delete operation.

This observer never admits, resumes, acknowledges, or repairs lifecycle work.
Each sample uses a fresh transaction. immutable=1 is deliberately forbidden:
ignoring a live WAL could falsely report an already completed Delete as Running.
The caller must retain samples and validate the full cleanup graph/receipts.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from installed_delete_e2e import request_hash
from installed_developer_startup import require

LIMIT = 2 * 1024 * 1024
OP_FIELDS = ("operation_id", "idempotency_key", "request_id", "project_id", "environment_id",
             "schema_version", "generation", "kind", "status", "request_hash", "definition_digest",
             "initial_state", "requested_target", "created_at", "updated_at", "completed_at")
TOMB_FIELDS = ("environment_id", "project_id", "schema_version", "name", "definition_digest",
               "delete_operation_id", "lifecycle_generation", "ownership_digest", "deleted_at")


def _projected(row, fields, payload):
    if row is None:
        return None
    raw = row[payload]
    require(isinstance(raw, str) and len(raw.encode()) <= LIMIT, "oversized/missing durable Delete JSON")
    value = json.loads(raw)
    require(isinstance(value, dict) and all(value.get(key) == row[key] for key in fields),
            "durable Delete SQL/JSON projection mismatch")
    return value


class DeleteStateReader:
    def __init__(self, path, environment, request, selector):
        self.path = Path(path)
        self.environment, self.request = environment, request
        self.expected_hash = request_hash(environment["project_id"], environment["environment_id"], selector)
        self.operation_id = None
        self.fd = None
        require(self.path.is_absolute() and self.path.resolve(strict=True) == self.path,
                "Delete evidence database path redirected")
        parent = self.path.parent.lstat()
        require(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.geteuid() and parent.st_mode & 0o077 == 0,
                "Delete evidence database parent must be private and owned")
        self.parent_identity = (parent.st_dev, parent.st_ino)
        try:
            self.fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
            value = os.fstat(self.fd)
            require(stat.S_ISREG(value.st_mode) and value.st_uid == os.geteuid() and value.st_mode & 0o077 == 0
                    and value.st_nlink == 1, "Delete evidence database must be private regular owned single-link file")
            self.file_identity = (value.st_dev, value.st_ino)
            self._identity()
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def _identity(self):
        require(self.fd is not None, "Delete evidence reader is closed")
        require(self.path.resolve(strict=True) == self.path, "Delete evidence database path redirected")
        parent, value = self.path.parent.lstat(), self.path.lstat()
        require((parent.st_dev, parent.st_ino) == self.parent_identity and
                stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.geteuid() and parent.st_mode & 0o077 == 0 and
                (value.st_dev, value.st_ino) == self.file_identity and
                stat.S_ISREG(value.st_mode) and value.st_nlink == 1 and value.st_uid == os.geteuid() and
                value.st_mode & 0o077 == 0, "Delete evidence database identity changed")
        # The live daemon creates WAL/SHM. A reader must not initialize its own
        # sidecars or follow an externally redirected sidecar.
        for suffix in ("-wal", "-shm"):
            path = Path(str(self.path) + suffix)
            value = path.lstat()
            require(stat.S_ISREG(value.st_mode) and value.st_uid == os.geteuid() and value.st_nlink == 1
                    and value.st_mode & 0o077 == 0, "Delete evidence WAL sidecar must be private and owned")

    def snapshot(self):
        self._identity()
        started = time.time_ns()
        connection = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=2, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            require(connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal",
                    "Delete evidence requires the live WAL database")
            deadline = time.monotonic() + 2
            connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            connection.execute("BEGIN")
            op = connection.execute(
                "SELECT " + ",".join(OP_FIELDS) + ", CASE WHEN length(operation_json) <= ? THEN operation_json END AS operation_json "
                "FROM environment_lifecycle_operations WHERE idempotency_key = ?", (LIMIT, self.request)).fetchone()
            operation = _projected(op, OP_FIELDS, "operation_json")
            tomb = connection.execute(
                "SELECT " + ",".join(TOMB_FIELDS) + ", CASE WHEN length(tombstone_json) <= ? THEN tombstone_json END AS tombstone_json "
                "FROM environment_tombstones WHERE environment_id = ?", (LIMIT, self.environment["environment_id"])).fetchone()
            tombstone = _projected(tomb, TOMB_FIELDS, "tombstone_json")
            env = connection.execute("SELECT project_id, lifecycle_generation, active_operation_id, "
                                     "CASE WHEN length(instance_json) <= ? THEN instance_json END AS instance_json "
                                     "FROM environment_instances WHERE environment_id = ?",
                                     (LIMIT, self.environment["environment_id"],)).fetchone()
            machines = connection.execute("SELECT machine_id FROM machine_instances WHERE environment_id = ? LIMIT 129",
                                          (self.environment["environment_id"],)).fetchall()
            require(len(machines) <= 128, "unexpected durable Delete Machine count")
            connection.execute("ROLLBACK")
        finally:
            connection.close()
        self._identity()
        require(operation is not None or self.operation_id is None, "observed durable Delete disappeared")
        if operation is not None:
            require(operation["project_id"] == self.environment["project_id"] and
                    operation["environment_id"] == self.environment["environment_id"] and
                    operation["request_id"] == operation["idempotency_key"] == self.request and
                    operation["request_hash"] == self.expected_hash and operation["schema_version"] == 1 and
                    operation["generation"] == self.environment["lifecycle_generation"] + 1 and
                    operation["definition_digest"] == self.environment["definition_digest"] and
                    operation["initial_state"] == self.environment["state"] and
                    operation["kind"] == "delete" and operation["requested_target"] == "deleted",
                    "durable Delete operation is not the exact admitted request")
            require(self.operation_id is None or self.operation_id == operation["operation_id"],
                    "durable Delete operation identity changed")
            self.operation_id = operation["operation_id"]
        if tombstone is not None:
            require(operation is not None and operation["status"] == "succeeded" and
                    all(tombstone[key] == self.environment[key] for key in ("project_id", "environment_id", "name", "definition_digest")) and
                    tombstone["schema_version"] == 1 and tombstone["delete_operation_id"] == operation["operation_id"] and
                    tombstone["lifecycle_generation"] == operation["generation"] and
                    tombstone["deleted_at"] == operation["completed_at"] and env is None and not machines,
                    "durable Delete tombstone does not bind exact removed Environment")
        if env is not None:
            require(env["project_id"] == self.environment["project_id"], "durable Delete Environment owner changed")
            require(isinstance(env["instance_json"], str) and len(env["instance_json"].encode()) <= LIMIT,
                    "oversized Environment evidence")
            current = json.loads(env["instance_json"])
            require(current["environment_id"] == self.environment["environment_id"] and
                    current["project_id"] == env["project_id"] and
                    current["lifecycle_generation"] == env["lifecycle_generation"] and
                    current.get("active_operation_id") == env["active_operation_id"],
                    "durable Delete Environment projections changed")
        return {"started_unix_ns": started, "observed_unix_ns": time.time_ns(), "operation": operation,
                "tombstone": tombstone, "environment_present": env is not None,
                "environment": json.loads(env["instance_json"]) if env is not None else None,
                "machine_ids": sorted(row["machine_id"] for row in machines),
                "database_identity": {"device": self.file_identity[0], "inode": self.file_identity[1]},
                "source": "read_only_live_wal_transaction_no_lifecycle_dispatch"}
