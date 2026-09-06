"""Independent offline recovery-evidence adversaries; no daemon, Docker or VM."""
import copy
import contextlib
import io
import json
import unittest
from unittest.mock import patch

import installed_daemon_recovery_e2e as driver
from test_installed_delete_quiescence import fixture as delete_fixture


def fixture():
    binding, final, proof = delete_fixture(stopped=True)
    environment = {key: final[key] for key in ("project_id", "environment_id", "definition_digest")}
    environment.update(state="ready", lifecycle_generation=1,
                       machines=[copy.deepcopy(binding["machine"])])
    request, selector = "stop-before-crash", environment["environment_id"]
    stop = copy.deepcopy(final)
    stop.update(operation_id="lop_prior_stop", kind="stop", generation=2,
                request_id=request, idempotency_key=request,
                request_hash=driver.stop_hash(environment, selector), initial_state="ready",
                requested_target="stopped", created_at=11, updated_at=18, completed_at=18,
                cleanup_steps=[])
    stop["machine_steps"][0].update(initial_state="ready", target_state="stopped")
    running = copy.deepcopy(stop)
    running.update(status="running", updated_at=12)
    running.pop("completed_at")
    running["machine_steps"][0]["status"] = "running"
    rows = [{"schema_version": 1, "record_type": "request_started", "operation": "stop_environment",
             "request_id": request, "idempotency_key": request}]
    rows.extend({"schema_version": 1, "record_type": "operation_progress", "request_id": request,
                 "idempotency_key": request, "sequence": sequence, "terminal": terminal,
                 "error": None, "operation": operation}
                for sequence, terminal, operation in ((3, False, running), (17, True, stop)))
    proof["authority"] = {"kind": "absent", "authority": {"kind": "positive_stop", "operation": copy.deepcopy(stop)}}
    return environment, binding, final, proof, stop, request, selector, rows


def encode(rows):
    return b"\n".join(json.dumps(row).encode() for row in rows) + b"\n"


def physical_stop():
    _, binding, _, _, stop, _, _, _ = fixture()
    receipt = {"owner": copy.deepcopy(binding["owner"]), "operation_id": stop["operation_id"],
               "generation": stop["generation"], "runtime_identity": json.loads(binding["runtime_identity"]["opaque_id"]),
               "outcome": "stopped", "endpoint": {"accepted_connections": 12, "completed_connections": 8,
                   "cancelled_connections": 1, "failed_connections": 3, "active_connections": 0, "socket_removed": True},
               "docker_shutdown": {"request_id": stop["operation_id"], "data_device": "/dev/vda",
                   "data_mount": "/var/lib/docker", "supervisor_started": True, "dockerd_reaped": True,
                   "containerd_reaped": True, "filesystem_synced": True, "filesystem_unmounted": True,
                   "never_started_unmounted": False, "filesystem_uuid": "31fd32e6-e95b-422c-973f-54c79ded35ea",
                   "filesystem_features": ["has_journal", "extent", "filetype"], "filesystem_state": "clean"}}
    return binding, stop, receipt


def process(pid=400, seconds=100):
    return {"pid": pid, "uid": 501, "start_seconds": seconds, "start_microseconds": 123456,
            "boot_session_uuid": "31fd32e6-e95b-422c-973f-54c79ded35ea"}


def owners():
    old = {"schema_version": 1, "daemon_id": "daemon-original", "process": process(),
           "configuration": {"socket_path": "/owned/runtime.sock", "state_store_path": "/owned/state.db",
                             "runtime_data_dir": "/owned/runtime", "log_path": "/owned/runtime.log",
                             "pid_path": "/owned/runtime.pid"}}
    for index, key in enumerate(("socket_parent", "state_parent", "runtime_root", "history_root", "database"), 1):
        old[key] = {"device": 1, "inode": index}
    for index, key in enumerate(("database_lock", "socket_lock", "log", "pid", "socket"), 10):
        old[key] = {"path": "/owned/" + key, "identity": {"device": 1, "inode": index}}
    old["socket"]["path"] = old["configuration"]["socket_path"]
    old["socket"]["staging_path"] = "/owned/.c12345678/s"
    old["staging_parent"] = {"device": 1, "inode": 30}
    new = copy.deepcopy(old)
    new.update(daemon_id="daemon-replacement", process=process(401, 200))
    new["socket"]["identity"]["inode"] = 40
    new["pid"]["identity"]["inode"] = 41
    previous, replacement = {"record": old, "sha256": "a" * 64}, {"record": new, "sha256": "b" * 64}
    receipt = {"schema_version": 1, "daemon_id": new["daemon_id"], "previous_daemon_id": old["daemon_id"],
               "previous_owner_sha256": previous["sha256"], "previous_process_observation": None,
               "graceful_closed": None, "scope": driver.CONTROL_SCOPE}
    return previous, replacement, receipt


class RecoveryEvidenceTests(unittest.TestCase):
    def test_physical_stop_requires_original_runtime_and_all_positive_boundaries(self):
        binding, stop, receipt = physical_stop()
        result = driver.validate_stop_receipt(binding, stop, receipt)
        self.assertEqual(result, receipt)
        result["docker_shutdown"]["filesystem_unmounted"] = False
        self.assertTrue(receipt["docker_shutdown"]["filesystem_unmounted"])

    def test_physical_stop_rejects_foreign_absent_or_forged_authority(self):
        mutations = (
            lambda r: r.update(extra=True), lambda r: r.pop("docker_shutdown"),
            lambda r: r.update(docker_shutdown=None), lambda r: r.update(outcome="already_absent"),
            lambda r: r.update(operation_id="foreign"), lambda r: r.update(generation=True),
            lambda r: r["owner"].update(machine_id="foreign"),
            lambda r: r["runtime_identity"].update(incarnation_id="replacement"),
            lambda r: r["docker_shutdown"].update(request_id="foreign"),
            lambda r: r["docker_shutdown"].update(data_device="/dev/vdb"),
            lambda r: r["docker_shutdown"].update(data_mount="/foreign"),
            lambda r: r["docker_shutdown"].update(never_started_unmounted=True),
            lambda r: r["docker_shutdown"].update(never_started_unmounted=0),
            lambda r: r["docker_shutdown"].update(extra=True))
        for mutate in mutations:
            binding, stop, receipt = physical_stop()
            mutate(receipt)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                driver.validate_stop_receipt(binding, stop, receipt)
        for key in ("supervisor_started", "dockerd_reaped", "containerd_reaped", "filesystem_synced", "filesystem_unmounted"):
            for value in (False, 1, None, "true"):
                binding, stop, receipt = physical_stop()
                receipt["docker_shutdown"][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    driver.validate_stop_receipt(binding, stop, receipt)

    def test_physical_stop_rejects_unjournaled_corrupt_or_unidentified_filesystem(self):
        for key, values in {
            "filesystem_uuid": [None, "", "foreign", "00000000-0000-0000-0000-000000000000"],
            "filesystem_state": [None, "not clean", "clean with errors", "not clean with errors"],
            "filesystem_features": [None, "has_journal extent", [], ["extent"], ["has_journal"],
                ["has_journal", "extent", "needs_recovery"],
                ["has_journal", "extent", "extent"], ["has_journal", "extent", True],
                ["has_journal", "extent", "bad feature"], ["ext_attr", "dir_index", "filetype", "sparse_super"]],
        }.items():
            for value in values:
                binding, stop, receipt = physical_stop()
                receipt["docker_shutdown"][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    driver.validate_stop_receipt(binding, stop, receipt)

    def test_restarted_machine_must_retain_original_filesystem_uuid_and_features(self):
        binding, stop, receipt = physical_stop()
        previous = {key: copy.deepcopy(receipt["docker_shutdown"][key])
                    for key in ("filesystem_uuid", "filesystem_features")}
        self.assertEqual(driver.validate_stop_receipt(binding, stop, receipt, previous), receipt)
        for key, value in (("filesystem_uuid", "41fd32e6-e95b-422c-973f-54c79ded35ea"),
                           ("filesystem_features", ["has_journal", "extent"])):
            changed = dict(previous, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate_stop_receipt(binding, stop, receipt, changed)

    def test_physical_stop_rejects_live_endpoint_or_nonmatching_public_stop(self):
        for key, value in (("socket_removed", False), ("socket_removed", 1), ("active_connections", 1),
                           ("active_connections", False), ("accepted_connections", 13), ("completed_connections", -1)):
            binding, stop, receipt = physical_stop()
            receipt["endpoint"][key] = value
            with self.subTest(endpoint=key), self.assertRaises(ValueError):
                driver.validate_stop_receipt(binding, stop, receipt)
        for mutate in (lambda o: o.update(status="running"), lambda o: o.update(kind="delete"),
                       lambda o: o.update(project_id="foreign"), lambda o: o.update(environment_id="foreign"),
                       lambda o: o["machine_steps"].clear(),
                       lambda o: o["machine_steps"].append(copy.deepcopy(o["machine_steps"][0])),
                       lambda o: o["machine_steps"][0].update(initial_state="stopped"),
                       lambda o: o["machine_steps"][0]["expected_incarnation"].update(generation=2)):
            binding, stop, receipt = physical_stop()
            mutate(stop)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                driver.validate_stop_receipt(binding, stop, receipt)

    def test_writable_layer_marker_uses_exact_host_docker_container_without_sync(self):
        harness = object.__new__(driver.RecoveryHarness)
        descriptor = {"name": "owned-context"}
        row = {"descriptor": descriptor, "container_id": "a" * 64, "token": "unique-marker"}
        with patch.object(driver.base.DeleteHarness, "sentinel", return_value=row), \
                patch.object(harness, "docker", side_effect=[(b"", b"", 0), (b"unique-marker\n", b"", 0)]) as docker:
            self.assertIs(harness.sentinel(descriptor), row)
        self.assertEqual(docker.call_count, 2)
        self.assertEqual(docker.call_args_list[0].args, (
            "sentinel-write-writable-layer", descriptor, ["exec", row["container_id"], "/bin/sh", "-c",
                'printf "%s\\n" "$1" > "$2"', "sh", row["token"], driver.WRITABLE_SENTINEL]))
        self.assertEqual(docker.call_args_list[1].args, (
            "sentinel-observe-writable-layer", descriptor,
            ["exec", row["container_id"], "/bin/cat", driver.WRITABLE_SENTINEL]))

    def test_writable_layer_marker_rejects_missing_changed_or_diagnostic_output(self):
        harness = object.__new__(driver.RecoveryHarness)
        row = {"descriptor": {"name": "owned-context"}, "container_id": "a" * 64, "token": "unique-marker"}
        for stdout, stderr in ((b"", b""), (b"unique-marker", b""), (b"other-marker\n", b""),
                               (b"unique-marker\n", b"untrusted warning")):
            with self.subTest(stdout=stdout, stderr=stderr), \
                    patch.object(harness, "docker", return_value=(stdout, stderr, 0)) as docker, \
                    self.assertRaises(ValueError):
                harness.check_writable_sentinel(row)
            docker.assert_called_once()

    def test_help_does_not_preflight_or_execute(self):
        with patch.object(driver, "preflight", side_effect=AssertionError("unexpected preflight")), \
                patch.object(driver, "run", side_effect=AssertionError("unexpected execution")), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as exit_status:
            driver.main(["--help"])
        self.assertEqual(exit_status.exception.code, 0)

    def test_complete_coalesced_stop_and_exact_prior_absence_proof(self):
        environment, binding, final, proof, stop, request, selector, rows = fixture()
        observed = driver.stop_terminal(encode(rows), environment, [binding], request, selector)
        self.assertEqual(observed, stop)
        validated = driver.validate_prior_stop_quiescence(binding, final, proof, observed)
        self.assertEqual(validated, proof)
        validated["authority"]["authority"]["operation"]["operation_id"] = "changed-return"
        self.assertEqual(proof["authority"]["authority"]["operation"]["operation_id"], stop["operation_id"])
        # The original binding stays Ready; the persisted Delete initial state is Stopped.
        self.assertEqual(binding["machine"]["state"], "ready")
        self.assertEqual(final["initial_state"], "stopped")

    def test_stop_rejects_uncorrelated_nonpositive_or_nonmonotonic_stream(self):
        mutations = [lambda r: r.pop(0), lambda r: r.pop(), lambda r: r.append(copy.deepcopy(r[-1])),
            lambda r: r[0].update(request_id="foreign"), lambda r: r[-1].update(request_id="foreign"),
            lambda r: r[-1].update(idempotency_key="foreign"), lambda r: r[-1].update(sequence=3),
            lambda r: r[-1].update(sequence=2), lambda r: r[-1].update(sequence=True),
            lambda r: r[-1].update(error={"code": "backend_unavailable"}),
            lambda r: r[-1]["operation"].update(status="blocked"),
            lambda r: r[-1]["operation"].update(completed_at=17),
            lambda r: r[-1]["operation"]["machine_steps"][0].update(status="running")]
        for index, mutate in enumerate(mutations):
            environment, binding, _, _, _, request, selector, rows = fixture()
            mutate(rows)
            with self.subTest(index=index), self.assertRaises(ValueError):
                driver.stop_terminal(encode(rows), environment, [binding], request, selector)

    def test_stop_rejects_each_changed_immutable_scope_and_machine(self):
        for key, value in {"operation_id": "foreign", "project_id": "foreign", "environment_id": "foreign",
                           "definition_digest": "sha256:" + "f" * 64, "generation": 3, "kind": "delete",
                           "request_id": "foreign", "idempotency_key": "foreign", "request_hash": "sha256:" + "f" * 64,
                           "initial_state": "stopped", "requested_target": "deleted", "created_at": 10}.items():
            environment, binding, _, _, _, request, selector, rows = fixture()
            rows[-1]["operation"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.stop_terminal(encode(rows), environment, [binding], request, selector)
        for mutate in (lambda o: o["machine_steps"].clear(),
                       lambda o: o["machine_steps"].append(copy.deepcopy(o["machine_steps"][0])),
                       lambda o: o["machine_steps"][0].update(machine_id="foreign"),
                       lambda o: o["machine_steps"][0]["expected_incarnation"].update(generation=2),
                       lambda o: o["machine_steps"][0].update(target_state="deleted"),
                       lambda o: o["machine_steps"][0].update(resulting_activation={}),
                       lambda o: o["machine_steps"][0].update(failure_reason="uncertain"),
                       lambda o: o["cleanup_steps"].append({"status": "succeeded"})):
            environment, binding, _, _, _, request, selector, rows = fixture()
            mutate(rows[-1]["operation"])
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                driver.stop_terminal(encode(rows), environment, [binding], request, selector)

    def test_absence_proof_cannot_forge_stop_or_delete_scope_or_owner(self):
        for mutate in (lambda p: p.update(schema_version=True), lambda p: p.update(extra=True),
                       lambda p: p["owner"].update(machine_id="foreign"),
                       lambda p: p.update(configuration_digest="sha256:" + "f" * 64),
                       lambda p: p["operation"].update(operation_id="foreign"),
                       lambda p: p["operation"].update(updated_at=31),
                       lambda p: p["operation"]["cleanup_steps"][0]["ownership"].update(resource_id="foreign"),
                       lambda p: p["operation"]["machine_steps"][0]["expected_incarnation"].update(generation=2),
                       lambda p: p["authority"].update(kind="drained"),
                       lambda p: p["authority"]["authority"].update(kind="acknowledged_delete"),
                       lambda p: p["authority"]["authority"]["operation"].update(operation_id="foreign"),
                       lambda p: p["authority"]["authority"]["operation"].update(status="running"),
                       lambda p: p["authority"]["authority"].update(extra=True)):
            _, binding, final, proof, stop, _, _, _ = fixture()
            mutate(proof)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                driver.validate_prior_stop_quiescence(binding, final, proof, stop)

    def test_prior_stop_must_be_immediate_completed_predecessor(self):
        for key, value in (("generation", 1), ("completed_at", 21), ("status", "running"),
                           ("kind", "delete"), ("environment_id", "foreign"), ("project_id", "foreign")):
            _, binding, final, proof, stop, _, _, _ = fixture()
            stop[key] = value
            proof["authority"]["authority"]["operation"] = copy.deepcopy(stop)
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate_prior_stop_quiescence(binding, final, proof, stop)

    def test_native_process_identity_is_exact_typed_bounded_birth(self):
        driver.validate_process_identity(process())
        for key, values in {"pid": [0, 1, True, -1, 2**31, "400"], "uid": [-1, True, 2**32],
                            "start_seconds": [0, -1, True, "100"],
                            "start_microseconds": [-1, True, 1000000],
                            "boot_session_uuid": [None, "", "foreign", "31FD32E6-e95b-422c-973f-54c79ded35ea"]}.items():
            for value in values:
                changed = process()
                changed[key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    driver.validate_process_identity(changed)
        for changed in ({}, dict(process(), unexpected=True)):
            with self.assertRaises(ValueError):
                driver.validate_process_identity(changed)

    def test_recovery_accepts_absent_zombie_or_reused_pid_not_original_live_birth(self):
        for observation in (None, {"identity": process(), "zombie": True},
                            {"identity": process(seconds=150), "zombie": False}):
            previous, replacement, receipt = owners()
            receipt["previous_process_observation"] = observation
            result = driver.validate_recovery_record(previous, replacement, receipt)
            self.assertEqual(result, receipt)
            result["scope"] = "changed-return"
            self.assertEqual(receipt["scope"], driver.CONTROL_SCOPE)
        for observation in ({"identity": process(), "zombie": False},
                            {"identity": process(), "zombie": 1},
                            {"identity": process(999), "zombie": True},
                            {"identity": process(), "zombie": True, "extra": True}):
            previous, replacement, receipt = owners()
            receipt["previous_process_observation"] = observation
            with self.subTest(observation=observation), self.assertRaises(ValueError):
                driver.validate_recovery_record(previous, replacement, receipt)

    def test_recovery_rejects_forged_control_record_and_persistence_replacement(self):
        for key, value in {"schema_version": True, "daemon_id": "foreign", "previous_daemon_id": "foreign",
                           "previous_owner_sha256": "f" * 64, "graceful_closed": {},
                           "scope": "all_original_VMs_absent", "extra": True}.items():
            previous, replacement, receipt = owners()
            receipt[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate_recovery_record(previous, replacement, receipt)
        for key in ("configuration", "socket_parent", "state_parent", "runtime_root", "history_root", "staging_parent", "database",
                    "database_lock", "socket_lock", "log"):
            previous, replacement, receipt = owners()
            replacement["record"][key]["changed"] = True
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate_recovery_record(previous, replacement, receipt)
        for key in ("process", "daemon_id", "socket"):
            previous, replacement, receipt = owners()
            replacement["record"][key] = copy.deepcopy(previous["record"][key])
            receipt["daemon_id"] = replacement["record"]["daemon_id"]
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate_recovery_record(previous, replacement, receipt)
        previous, replacement, receipt = owners()
        replacement["record"]["socket"]["path"] = "/foreign/runtime.sock"
        with self.assertRaises(ValueError):
            driver.validate_recovery_record(previous, replacement, receipt)

    def test_positive_close_requires_exact_replacement_owner_and_both_absences(self):
        _, replacement, _ = owners()
        receipt = {"schema_version": 1, "daemon_id": replacement["record"]["daemon_id"],
                   "owner_sha256": replacement["sha256"], "socket_removed": True, "pid_removed": True,
                   "scope": driver.CONTROL_SCOPE}
        self.assertEqual(driver.validate_closed_record(replacement, receipt), receipt)
        for key, value in {"schema_version": True, "daemon_id": "daemon-original", "owner_sha256": "a" * 64,
                           "socket_removed": False, "pid_removed": 1, "scope": "VM_quiescence", "extra": True}.items():
            changed = dict(receipt, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate_closed_record(replacement, changed)

    def test_recovery_rejects_cross_boot_or_changed_socket_staging_authority(self):
        other_boot = "41fd32e6-e95b-422c-973f-54c79ded35ea"
        for target in ("replacement", "observation", "staging_path"):
            previous, replacement, receipt = owners()
            if target == "replacement":
                replacement["record"]["process"]["boot_session_uuid"] = other_boot
            elif target == "observation":
                receipt["previous_process_observation"] = {
                    "identity": dict(process(), boot_session_uuid=other_boot), "zombie": True}
            else:
                replacement["record"]["socket"]["staging_path"] = "/foreign/s"
            with self.subTest(target=target), self.assertRaises(ValueError):
                driver.validate_recovery_record(previous, replacement, receipt)

    def test_uncertain_recovery_withholds_automatic_cleanup(self):
        harness = object.__new__(driver.RecoveryHarness)
        harness.recovery_pending = True
        with patch.object(driver.base.DeleteHarness, "cleanup", side_effect=AssertionError("unsafe cleanup")), \
                self.assertRaises(ValueError):
            harness.cleanup()


if __name__ == "__main__":
    unittest.main()
