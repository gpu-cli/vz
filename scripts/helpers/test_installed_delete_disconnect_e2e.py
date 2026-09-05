"""Independent offline disconnect-evidence tests; no processes, VM, or SQL writes."""
import copy
from contextlib import redirect_stdout
import io
import signal
import unittest
from unittest.mock import Mock, patch

import installed_delete_disconnect_e2e as driver
from test_installed_delete_e2e import fixture as terminal_fixture


def fixture():
    environment, bindings, request, terminal = terminal_fixture()
    final = terminal[-1]["operation"]
    final.update(created_at=10, updated_at=20)
    admitted = copy.deepcopy(final)
    admitted.update(status="running", updated_at=11)
    admitted.pop("completed_at")
    for step in admitted["machine_steps"] + admitted["cleanup_steps"]:
        step["status"] = "pending"
    rows = [copy.deepcopy(terminal[0]), dict(copy.deepcopy(terminal[-1]), operation=admitted,
                                          sequence=7, terminal=False, tombstone=None)]
    current = copy.deepcopy(environment)
    current.update(state="deleting", lifecycle_generation=admitted["generation"],
                   active_operation_id=admitted["operation_id"])
    before = {"started_unix_ns": 10, "observed_unix_ns": 20, "operation": copy.deepcopy(admitted),
              "tombstone": None, "environment_present": True, "environment": current,
              "machine_ids": sorted(x["machine_id"] for x in admitted["machine_steps"]),
              "database_identity": {"device": 1, "inode": 2},
              "source": "read_only_live_wal_transaction_no_lifecycle_dispatch"}
    after = copy.deepcopy(before)
    after.update(started_unix_ns=40, observed_unix_ns=50)
    completion = copy.deepcopy(after)
    completion.update(started_unix_ns=60, observed_unix_ns=70, operation=copy.deepcopy(final),
                      tombstone=copy.deepcopy(terminal[-1]["tombstone"]), environment_present=False,
                      environment=None, machine_ids=[])
    return environment, bindings, request, rows, admitted, before, after, completion


def disconnected(before, after, admitted):
    return driver.validate_disconnect(before, after, admitted, -signal.SIGTERM, signal.SIGTERM, 30)


class DisconnectEvidenceTests(unittest.TestCase):
    def test_help_resolves_parser_without_preflight_or_dispatch(self):
        with patch.object(driver, "preflight", side_effect=AssertionError("help must be read-only")), redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                driver.main(["--help"])
            self.assertEqual(stopped.exception.code, 0)

    def test_nonterminal_admission_accepts_coalesced_gaps_and_progress(self):
        environment, bindings, request, rows, admitted, *_ = fixture()
        progress = copy.deepcopy(rows[-1])
        progress["sequence"] = 900
        progress["operation"]["updated_at"] += 1
        progress["operation"]["machine_steps"][0]["status"] = "succeeded"
        self.assertEqual(driver.validate_admission(rows + [progress], environment, bindings, request, "primary"),
                         progress["operation"])
        self.assertEqual(driver.immutable_scope(admitted), driver.immutable_scope(progress["operation"]))

    def test_buffered_terminal_missing_preamble_and_duplicate_frames_cannot_prove_disconnect(self):
        environment, bindings, request, rows, *_ = fixture()
        variants = [rows[:1], rows[1:], rows + [rows[-1]]]
        for field, value in (("terminal", True), ("error", {"code": "timeout"}),
                             ("tombstone", {"deleted_at": 20}), ("request_id", "foreign"),
                             ("sequence", True), ("sequence", -1)):
            changed = copy.deepcopy(rows)
            changed[-1][field] = value
            variants.append(changed)
        for index, changed in enumerate(variants):
            with self.subTest(index=index), self.assertRaises(ValueError):
                driver.validate_admission(changed, environment, bindings, request, "primary")

    def test_admission_pins_request_project_scope_machine_incarnation_and_cleanup(self):
        for mutate in (
            lambda op: op.update(request_id="foreign"), lambda op: op.update(idempotency_key="foreign"),
            lambda op: op.update(project_id="prj_foreign"), lambda op: op.update(environment_id="env_foreign"),
            lambda op: op.update(generation=9), lambda op: op.update(request_hash="sha256:" + "f" * 64),
            lambda op: op["machine_steps"][0]["expected_incarnation"].update(created_at=99),
            lambda op: op["machine_steps"][0].update(resulting_activation={"foreign": True}),
            lambda op: op["cleanup_steps"].pop(),
            lambda op: op["cleanup_steps"][0]["ownership"].update(resource_id="foreign"),
        ):
            environment, bindings, request, rows, *_ = fixture()
            mutate(rows[-1]["operation"])
            with self.assertRaises(ValueError):
                driver.validate_admission(rows, environment, bindings, request, "primary")

    def test_fresh_post_reap_running_snapshot_is_positive_disconnect_evidence(self):
        _, _, _, _, admitted, before, after, _ = fixture()
        after["operation"]["machine_steps"][0]["status"] = "succeeded"
        after["operation"]["updated_at"] += 1
        result = disconnected(before, after, admitted)
        self.assertEqual(result["observer_exit_code"], -signal.SIGTERM)
        self.assertEqual(result["observer_reaped_unix_ns"], 30)
        self.assertEqual(result["after_observer_reap"], after)

    def test_pre_reap_snapshot_even_when_finished_later_is_not_fresh(self):
        for field, value in (("started_unix_ns", 29), ("started_unix_ns", 30),
                             ("started_unix_ns", 51), ("started_unix_ns", True),
                             ("observed_unix_ns", 39)):
            _, _, _, _, admitted, before, after, _ = fixture()
            after[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                disconnected(before, after, admitted)
        for code, sent, reap in ((0, signal.SIGTERM, 30), (-signal.SIGKILL, signal.SIGKILL, 30),
                                (-signal.SIGTERM, signal.SIGTERM, 20),
                                (-signal.SIGTERM, signal.SIGTERM, True)):
            _, _, _, _, admitted, before, after, _ = fixture()
            with self.subTest(code=code, sent=sent, reap=reap), self.assertRaises(ValueError):
                driver.validate_disconnect(before, after, admitted, code, sent, reap)

    def test_already_completed_missed_window_or_missing_running_authority_fails(self):
        for mutate in (
            lambda s: s.update(operation=None), lambda s: s["operation"].update(status="succeeded", completed_at=20),
            lambda s: s["operation"].update(status="blocked"), lambda s: s.update(tombstone={"deleted_at": 20}),
            lambda s: s.update(environment_present=False), lambda s: s["machine_ids"].pop(),
            lambda s: s["operation"].update(operation_id="lop_other"),
            lambda s: s["operation"].update(created_at=9),
        ):
            _, _, _, _, admitted, before, after, _ = fixture()
            mutate(after)
            with self.assertRaises(ValueError):
                disconnected(before, after, admitted)

    def test_running_snapshot_requires_exact_active_environment_database_and_live_wal_source(self):
        for mutate in (
            lambda s: s["environment"].update(active_operation_id="lop_foreign"),
            lambda s: s["environment"].update(lifecycle_generation=9),
            lambda s: s["environment"].update(environment_id="env_foreign"),
            lambda s: s["environment"].update(project_id="prj_foreign"),
            lambda s: s["environment"].update(state="ready"),
            lambda s: s.update(environment=None), lambda s: s["database_identity"].update(inode=999),
            lambda s: s.update(source="immutable_database_stale_snapshot"),
            lambda s: s["operation"]["machine_steps"][0].update(resulting_incarnation={"foreign": True}),
            lambda s: s["operation"]["machine_steps"][0].update(resulting_activation={"foreign": True}),
            lambda s: s["operation"]["cleanup_steps"][0].update(status="failed", failure_reason="unproven"),
        ):
            _, _, _, _, admitted, before, after, _ = fixture()
            mutate(after)
            with self.subTest(after=after), self.assertRaises(ValueError):
                disconnected(before, after, admitted)

    def test_durable_completion_returns_exact_original_tombstone_without_replay(self):
        environment, bindings, request, _, admitted, _, _, completion = fixture()
        result = driver.validate_completion(completion, admitted, environment, bindings, request, "primary")
        self.assertEqual(result, {"operation": completion["operation"], "tombstone": completion["tombstone"],
                                  "request_id": request})

    def test_completion_cannot_use_foreign_tombstone_unfinished_cleanup_or_present_aggregate(self):
        for mutate in (
            lambda s: s.update(tombstone=None), lambda s: s.update(environment_present=True),
            lambda s: s.update(machine_ids=["mch_survivor"]),
            lambda s: s["operation"].update(operation_id="lop_foreign"),
            lambda s: s["operation"]["cleanup_steps"][0].update(status="pending"),
            lambda s: s["tombstone"].update(delete_operation_id="lop_foreign"),
            lambda s: s["tombstone"].update(ownership_digest="sha256:" + "f" * 64),
            lambda s: s["tombstone"].update(deleted_at=21),
            lambda s: s["tombstone"].update(environment_id="env_foreign"),
        ):
            environment, bindings, request, _, admitted, _, _, completion = fixture()
            mutate(completion)
            with self.assertRaises(ValueError):
                driver.validate_completion(completion, admitted, environment, bindings, request, "primary")

    def test_unreaped_observer_withholds_lifecycle_cleanup_without_signaling(self):
        harness = object.__new__(driver.DisconnectHarness)
        harness.observer_process = Mock()
        harness.observer_process.poll.return_value = None
        with patch.object(driver.base.DeleteHarness, "cleanup", side_effect=AssertionError("unsafe cleanup")), \
                patch.object(driver.os, "kill", side_effect=AssertionError("unexpected signal")):
            with self.assertRaises(ValueError):
                harness.cleanup()


if __name__ == "__main__":
    unittest.main()
