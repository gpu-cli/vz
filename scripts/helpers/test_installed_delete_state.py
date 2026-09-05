"""Real SQLite/WAL offline regressions; no daemon, Docker, VM or release claim."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import installed_delete_state as state
from test_installed_delete_e2e import fixture


class DeleteStateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / "state.db"
        self.writer = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.writer.close)
        self.writer.execute("PRAGMA journal_mode=WAL")
        self.writer.execute("PRAGMA wal_autocheckpoint=0")
        for table, fields, payload in (("environment_lifecycle_operations", state.OP_FIELDS, "operation_json"),
                                       ("environment_tombstones", state.TOMB_FIELDS, "tombstone_json")):
            integers = {"schema_version", "generation", "created_at", "updated_at", "completed_at", "lifecycle_generation", "deleted_at"}
            self.writer.execute("CREATE TABLE " + table + " (" + ",".join(key + (" INTEGER" if key in integers else " TEXT")
                for key in fields) + ", " + payload + " TEXT)")
        self.writer.execute("CREATE TABLE environment_instances (environment_id TEXT, project_id TEXT, lifecycle_generation INTEGER, active_operation_id TEXT, instance_json TEXT)")
        self.writer.execute("CREATE TABLE machine_instances (machine_id TEXT, environment_id TEXT)")
        for path in self.root.iterdir():
            path.chmod(0o600)
        self.environment, _, self.request, rows = fixture()
        self.terminal, self.tombstone = copy.deepcopy(rows[-1]["operation"]), rows[-1]["tombstone"]
        self.terminal.update(created_at=10, updated_at=20)
        self.running = dict(self.terminal, status="running", completed_at=None, updated_at=10)
        current = dict(self.environment, state="deleting", lifecycle_generation=2, active_operation_id=self.running["operation_id"])
        self.writer.execute("INSERT INTO environment_instances VALUES (?,?,?,?,?)", (
            current["environment_id"], current["project_id"], 2, current["active_operation_id"], json.dumps(current)))
        for machine in self.environment["machines"]:
            self.writer.execute("INSERT INTO machine_instances VALUES (?,?)", (machine["machine_id"], current["environment_id"]))
        self.insert_operation(self.running)
        self.reader = state.DeleteStateReader(self.path, self.environment, self.request, "primary")
        self.addCleanup(self.reader.close)

    def insert_operation(self, operation):
        self.writer.execute("DELETE FROM environment_lifecycle_operations")
        self.writer.execute("INSERT INTO environment_lifecycle_operations VALUES (" + ",".join("?" for _ in range(len(state.OP_FIELDS) + 1)) + ")",
                            tuple(operation[key] for key in state.OP_FIELDS) + (json.dumps(operation),))

    def complete(self):
        self.writer.execute("BEGIN")
        self.insert_operation(self.terminal)
        self.writer.execute("INSERT INTO environment_tombstones VALUES (" + ",".join("?" for _ in range(len(state.TOMB_FIELDS) + 1)) + ")",
                            tuple(self.tombstone[key] for key in state.TOMB_FIELDS) + (json.dumps(self.tombstone),))
        self.writer.execute("DELETE FROM environment_instances")
        self.writer.execute("DELETE FROM machine_instances")
        self.writer.execute("COMMIT")

    def test_fresh_wal_snapshots_see_completion_without_replay_or_checkpoint(self):
        before = self.reader.snapshot()
        self.assertEqual(before["operation"], self.running)
        self.assertIsNone(before["tombstone"])
        self.assertTrue(before["environment_present"])
        self.complete()
        after = self.reader.snapshot()
        self.assertEqual(after["operation"], self.terminal)
        self.assertEqual(after["tombstone"], self.tombstone)
        self.assertFalse(after["environment_present"])
        self.assertEqual(after["machine_ids"], [])
        self.assertGreater(after["started_unix_ns"], before["observed_unix_ns"])

    def test_sql_projection_corruption_rejected(self):
        self.writer.execute("UPDATE environment_lifecycle_operations SET status='succeeded'")
        with self.assertRaisesRegex(ValueError, "projection mismatch"):
            self.reader.snapshot()

    def test_exact_request_and_generation_are_bound(self):
        for key, value in (("request_hash", "foreign"), ("request_id", "foreign"), ("generation", 99),
                           ("project_id", "foreign"), ("environment_id", "foreign")):
            with self.subTest(key=key):
                self.insert_operation(dict(self.running, **{key: value}))
                with self.assertRaisesRegex(ValueError, "exact admitted"):
                    self.reader.snapshot()

    def test_tombstone_cannot_certify_remaining_machine(self):
        self.complete()
        self.writer.execute("INSERT INTO machine_instances VALUES (?,?)", ("mch_leaked", self.environment["environment_id"]))
        with self.assertRaisesRegex(ValueError, "exact removed"):
            self.reader.snapshot()

    def test_observed_operation_cannot_disappear(self):
        self.reader.snapshot()
        self.writer.execute("DELETE FROM environment_lifecycle_operations")
        with self.assertRaisesRegex(ValueError, "disappeared"):
            self.reader.snapshot()

    def test_replaced_database_or_symlink_rejected_without_reading_replacement(self):
        original = self.root / "retained.db"
        self.path.rename(original)
        self.path.symlink_to(original)
        with self.assertRaisesRegex(ValueError, "redirected"):
            self.reader.snapshot()

    def test_missing_sidecar_fails_before_sqlite_can_create_it(self):
        sidecar = Path(str(self.path) + "-shm")
        retained = self.root / "retained-shm"
        sidecar.rename(retained)
        try:
            with self.assertRaises(FileNotFoundError):
                self.reader.snapshot()
            self.assertFalse(sidecar.exists())
        finally:
            retained.rename(sidecar)

    def test_reader_uri_and_connection_reject_writes(self):
        original_connect = sqlite3.connect
        captured = []
        def connect(database_uri, **kwargs):
            self.assertTrue(database_uri.endswith("?mode=ro"))
            self.assertNotIn("immutable", database_uri)
            conn = original_connect(database_uri, **kwargs)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("DELETE FROM machine_instances")
            captured.append(database_uri)
            return conn
        with patch.object(state.sqlite3, "connect", side_effect=connect):
            self.reader.snapshot()
        self.assertEqual(len(captured), 1)

    def test_environment_generation_projection_mismatch_rejected(self):
        self.writer.execute("UPDATE environment_instances SET lifecycle_generation=9")
        with self.assertRaisesRegex(ValueError, "Environment projections"):
            self.reader.snapshot()

    def test_changed_parent_permissions_rejected(self):
        self.root.chmod(0o750)
        try:
            with self.assertRaisesRegex(ValueError, "identity changed"):
                self.reader.snapshot()
        finally:
            self.root.chmod(0o700)

    def test_fifo_database_is_rejected_without_waiting_for_writer(self):
        path = self.root / "not-a-database"
        os.mkfifo(path, 0o600)
        with self.assertRaisesRegex(ValueError, "regular owned"):
            state.DeleteStateReader(path, self.environment, self.request, "primary")


if __name__ == "__main__":
    unittest.main()
