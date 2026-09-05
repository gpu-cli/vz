"""Adversarial offline quiescence replay. No Docker, daemon, VM, or filesystem mutation."""
import copy
import json
import unittest

from installed_delete_quiescence import validate


def fixture(stopped=False):
    owner = {"project_id": "prj_test", "environment_id": "env_test", "machine_id": "mch_test"}
    incarnation = {"schema_version": 1, "machine_id": "mch_test", "incarnation_id": "inc_test",
                   "generation": 1, "created_at": 10}
    runtime = {"schema_version": 1, "stack_id": "owned-vm", "incarnation_id": "31fd32e6-e95b-422c-973f-54c79ded35ea"}
    rows = [{"schema_version": 1, "resource_kind": kind, "resource_id": name,
             "environment_id": "env_test", "machine_id": "mch_test"} for kind, name in
            [("docker_context", "owned-context"), ("incarnation", "inc_test"), ("machine", "mch_test"),
             ({"other": "machine_runtime_store"}, "owned-store"), ({"other": "runtime_vm"}, "owned-vm")]]
    binding = {"owner": owner, "machine": {"machine_id": "mch_test", "state": "ready"},
               "incarnation": incarnation, "runtime_identity": {"schema_version": 1, "opaque_id": json.dumps(runtime)},
               "ownership": rows, "manifest": {"configuration_digest": "sha256:" + "a" * 64}}
    state = "stopped" if stopped else "ready"
    operation = {"schema_version": 1, "operation_id": "lop_test", "project_id": "prj_test", "environment_id": "env_test",
                 "kind": "delete", "generation": 3 if stopped else 2, "request_id": "delete-test",
                 "idempotency_key": "delete-test", "request_hash": "sha256:" + "b" * 64,
                 "definition_digest": "sha256:" + "c" * 64, "initial_state": state, "requested_target": "deleted",
                 "status": "succeeded", "created_at": 20, "updated_at": 30, "completed_at": 30,
                 "machine_steps": [{"machine_id": "mch_test", "initial_state": state, "expected_incarnation": incarnation,
                                    "status": "succeeded"}],
                 "cleanup_steps": [{"ownership": row, "status": "succeeded"} for row in rows]}
    prior = copy.deepcopy(operation)
    prior.update(status="running", updated_at=22)
    prior.pop("completed_at")
    prior["machine_steps"][0]["status"] = "pending"
    for step in prior["cleanup_steps"]:
        step["status"] = "pending"
    proof = {"schema_version": 1, "owner": copy.deepcopy(owner), "configuration_digest": binding["manifest"]["configuration_digest"],
             "operation": prior, "authority": {"kind": "drained", "runtime_identity": runtime,
                 "outcome": "already_absent" if stopped else "stopped", "endpoint": {
                     "accepted_connections": 12, "completed_connections": 8, "cancelled_connections": 1,
                     "failed_connections": 3, "active_connections": 0, "socket_removed": True}}}
    return binding, operation, proof


class QuiescenceTests(unittest.TestCase):
    def test_ready_and_rebound_stopped_proofs_accept_only_mutable_progress_differences(self):
        for stopped in (False, True):
            binding, operation, proof = fixture(stopped)
            self.assertEqual(binding["machine"]["state"], "ready")
            validated = validate(binding, operation, proof)
            self.assertEqual(validated, proof)
            validated["owner"]["machine_id"] = "do-not-mutate-input"
            self.assertEqual(proof["owner"]["machine_id"], "mch_test")
            proof["operation"]["machine_steps"][0]["status"] = "succeeded"
            proof["operation"]["updated_at"] = 25
            validate(binding, operation, proof)

    def test_missing_unknown_foreign_and_untyped_outer_authority_rejected(self):
        for mutate in (
            lambda p: p.clear(), lambda p: p.update(schema_version=2), lambda p: p.update(schema_version=True),
            lambda p: p.update(extra=True), lambda p: p["owner"].update(machine_id="mch_foreign"),
            lambda p: p["owner"].update(project_id="prj_foreign"),
            lambda p: p.update(configuration_digest="sha256:" + "f" * 64),
            lambda p: p.update(authority={"kind": "acknowledged_delete"}),
            lambda p: p.update(authority={"kind": "absent", "authority": {"kind": "positive_stop"}}),
            lambda p: p["authority"].update(extra=True), lambda p: p["authority"].update(kind="unknown"),
        ):
            binding, operation, proof = fixture()
            mutate(proof)
            with self.subTest(proof=proof), self.assertRaises(ValueError):
                validate(binding, operation, proof)

    def test_each_immutable_operation_field_and_ownership_change_is_rejected(self):
        for key, value in {"operation_id": "lop_other", "project_id": "prj_other", "environment_id": "env_other",
                           "kind": "stop", "generation": 9, "request_id": "other", "idempotency_key": "other",
                           "request_hash": "sha256:" + "f" * 64, "definition_digest": "sha256:" + "f" * 64,
                           "initial_state": "stopped", "requested_target": "stopped", "created_at": 19}.items():
            binding, operation, proof = fixture()
            proof["operation"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(binding, operation, proof)
        for mutate in (
            lambda p: p["machine_steps"][0].update(target_state="stopped"),
            lambda p: p["machine_steps"][0]["expected_incarnation"].update(created_at=11),
            lambda p: p["machine_steps"].append(copy.deepcopy(p["machine_steps"][0])),
            lambda p: p["cleanup_steps"].pop(),
            lambda p: p["cleanup_steps"][0]["ownership"].update(resource_id="foreign"),
            lambda p: p["cleanup_steps"][0]["ownership"].update(machine_id="mch_foreign"),
            lambda p: p["cleanup_steps"][0]["ownership"].update(schema_version=True),
        ):
            binding, operation, proof = fixture()
            mutate(proof["operation"])
            with self.assertRaises(ValueError):
                validate(binding, operation, proof)

    def test_failed_unfinished_and_nonmonotonic_operation_receipts_rejected(self):
        for target, field, value in (
            ("proof", "status", "blocked"), ("proof", "completed_at", 30),
            ("proof", "updated_at", 31), ("proof", "updated_at", 19),
            ("proof", "generation", True), ("final", "status", "running"),
            ("final", "completed_at", 29), ("final", "completed_at", None),
        ):
            binding, operation, proof = fixture()
            (proof["operation"] if target == "proof" else operation)[field] = value
            with self.subTest(target=target, field=field), self.assertRaises(ValueError):
                validate(binding, operation, proof)
        for key in ("machine_steps", "cleanup_steps"):
            binding, operation, proof = fixture()
            proof["operation"][key][0].update(status="failed", failure_reason="uncertain")
            with self.assertRaises(ValueError):
                validate(binding, operation, proof)

    def test_foreign_runtime_incarnation_replacement_and_wrong_positive_outcome_rejected(self):
        for stopped in (False, True):
            for mutate in (
                lambda a: a["runtime_identity"].update(stack_id="foreign-vm"),
                lambda a: a["runtime_identity"].update(incarnation_id="foreign-boot"),
                lambda a: a["runtime_identity"].update(schema_version=True),
                lambda a: a.update(outcome={"replacement_present": {"current": "foreign"}}),
                lambda a: a.update(outcome="already_absent" if not stopped else "stopped"),
                lambda a: a.update(runtime_identity=None),
            ):
                binding, operation, proof = fixture(stopped)
                mutate(proof["authority"])
                with self.subTest(stopped=stopped), self.assertRaises(ValueError):
                    validate(binding, operation, proof)

    def test_endpoint_requires_exact_positive_socket_and_complete_integer_accounting(self):
        variants = [{}, None, [], {"socket_removed": True}]
        for key, value in (("socket_removed", False), ("socket_removed", 1), ("active_connections", 1),
                           ("active_connections", False), ("accepted_connections", 11),
                           ("completed_connections", -1), ("failed_connections", True),
                           ("cancelled_connections", 2**64), ("accepted_connections", 12.0), ("unknown", 0)):
            _, _, proof = fixture()
            endpoint = proof["authority"]["endpoint"]
            endpoint[key] = value
            variants.append(endpoint)
        for endpoint in variants:
            binding, operation, proof = fixture()
            proof["authority"]["endpoint"] = endpoint
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                validate(binding, operation, proof)

    def test_public_original_incarnation_and_vm_reservation_are_independent_authorities(self):
        for mode in ("incarnation", "vm", "owner"):
            binding, operation, proof = fixture()
            if mode == "incarnation":
                binding["incarnation"] = dict(binding["incarnation"], created_at=11)
            elif mode == "vm":
                binding["ownership"] = copy.deepcopy(binding["ownership"])
                binding["ownership"][-1]["resource_id"] = "another-vm"
            else:
                binding["machine"]["machine_id"] = "mch_foreign"
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                validate(binding, operation, proof)


if __name__ == "__main__":
    unittest.main()
