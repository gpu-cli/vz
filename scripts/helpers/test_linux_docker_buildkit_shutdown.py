"""Offline adversarial replay, not a physical normal-stop claim."""
import copy
import itertools
import json
import unittest

import linux_docker_buildkit_shutdown as shutdown


SINCE = "2026-09-06T07:18:56.100000000Z"
UNTIL = "2026-09-06T07:18:56.900000000Z"
START = "2026-09-06T07:18:55.131709236Z"
# Final six JSON-file log records independently extracted read-only from the
# failed candidate 5's stopped disk. Docker logs --timestamps renders their time
# and log fields; no guest retry or synthetic formatter supplied this trailer.
ACTUAL_MESSAGES = (
    ('2026-09-06T07:18:56.280884819Z', 'time="2026-09-06T07:18:56Z" level=info msg="stopping server"'),
    ('2026-09-06T07:18:56.281221153Z', 'buildkitd: got 1 SIGTERM/SIGINTs, forcing shutdown'),
    ('2026-09-06T07:18:56.281224819Z', 'github.com/moby/buildkit/util/appcontext.Context.func1.1'),
    ('2026-09-06T07:18:56.281225778Z', '\tgithub.com/moby/buildkit/util/appcontext/appcontext.go:38'),
    ('2026-09-06T07:18:56.281226861Z', 'runtime.goexit'),
    ('2026-09-06T07:18:56.281227528Z', '\truntime/asm_arm64.s:1223'),
)
ACTUAL_LOG = "".join(stamp + " " + message + "\n" for stamp, message in ACTUAL_MESSAGES).encode()


def records(container_id="b" * 64, name="builder", image_id="sha256:" + "a" * 64, token="token"):
    result = []
    for action, attrs in (("kill", {"signal": "15"}), ("die", {"exitCode": "1", "execDuration": "1"}), ("stop", {})):
        result.append({"type": "container", "action": action, "id": container_id, "scope": "local",
                       "time_nano": shutdown.timestamp("2026-09-06T07:18:56.300000000Z"),
                       "attributes": {shutdown.LABEL: token, "name": name, "image": image_id, **attrs}})
    return result


def encode(records):
    return b"".join(json.dumps(item).encode() + b"\n" for item in records)


def fixture():
    before = {"Id": "b" * 64, "Image": "sha256:" + "a" * 64, "Name": "/builder", "RestartCount": 0,
              "State": {"Running": True, "Status": "running", "Pid": 734, "StartedAt": START,
                        "Paused": False, "Restarting": False, "OOMKilled": False, "Dead": False, "Error": ""}}
    after = copy.deepcopy(before)
    after["State"].update(Running=False, Status="exited", Pid=0, ExitCode=1,
                          FinishedAt="2026-09-06T07:18:56.282503111Z")
    return dict(before=before, after=after, engine_since=SINCE, engine_until=UNTIL, stop_elapsed_ns=180000000,
                event_bytes=encode(records()), log_stdout=b"", log_stderr=ACTUAL_LOG, token="token")


class ShutdownTests(unittest.TestCase):
    def test_actual_final_log_and_every_racing_event_order(self):
        for permutation in itertools.permutations(records()):
            args = fixture()
            args["event_bytes"] = encode(permutation)
            result = shutdown.validate(**args)
            self.assertEqual(result["exit_code"], 1)
            self.assertEqual(result["pid_before_stop"], 734)
            self.assertEqual(result["source_commit"], shutdown.SOURCE_COMMIT)
            self.assertEqual(result["stderr_sha256"], shutdown.hashlib.sha256(ACTUAL_LOG).hexdigest())

    def test_python39_nanosecond_calendar_validation(self):
        self.assertEqual(shutdown.timestamp(SINCE), 1788679136100000000)
        for fraction in ("", ".1", ".12", ".123456", ".123456789"):
            shutdown.timestamp("2026-09-06T07:18:56" + fraction + "Z")
        for value in (True, None, "2026-02-30T07:18:56.123456789Z", "2026-09-06T25:18:56Z",
                      "2026-09-06T07:18:56.1234567890Z", "2026-09-06T07:18:56+00:00", "0001-01-01T00:00:00Z"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                shutdown.timestamp(value)

    def test_log_collection_can_cross_emission_second_boundary(self):
        raw = "".join("2026-09-06T07:18:57.00000000" + str(index + 1) + "Z " + message + "\n"
                      for index, (_, message) in enumerate(ACTUAL_MESSAGES)).encode()
        since = shutdown.timestamp("2026-09-06T07:18:56.900000000Z")
        until = shutdown.timestamp("2026-09-06T07:18:57.100000000Z")
        shutdown.logs(raw, since, until)
        for changed in (raw.replace(b'time="2026-09-06T07:18:56Z"', b'time="2026-09-06T07:18:55Z"'),
                        raw.replace(b'time="2026-09-06T07:18:56Z"', b'time="2026-09-06T07:18:58Z"')):
            with self.assertRaises(ValueError):
                shutdown.logs(changed, since, until)

    def test_wrong_source_binary_version_and_platform_rejected(self):
        contract = {"buildkit_version": "0.19.0", "source_commit": shutdown.SOURCE_COMMIT,
                    "buildkitd_sha256": shutdown.DAEMON_SHA256, "platform": "linux/arm64"}
        shutdown.source_contract(contract)
        for key in contract:
            with self.subTest(key=key), self.assertRaises(ValueError):
                shutdown.source_contract(dict(contract, **{key: "foreign"}))

    def test_abnormal_lifecycle_and_boolean_numeric_values_rejected(self):
        changes = [("State", k, v) for k, values in {
            "Pid": [True, 1], "ExitCode": [True, 0, 137, 143, "1"], "Running": [True, 0],
            "Status": ["dead", "running"], "OOMKilled": [True, None], "Paused": [True], "Restarting": [True],
            "Dead": [True], "Error": ["OCI failure"], "StartedAt": ["2026-09-06T07:18:55.2Z"],
            "FinishedAt": [START, "2026-09-06T07:18:57Z"]}.items() for v in values]
        changes += [(None, "RestartCount", v) for v in (True, 1, "0")]
        changes += [(None, k, "foreign") for k in ("Id", "Name", "Image")]
        for parent, key, value in changes:
            args = fixture()
            target = args["after"][parent] if parent else args["after"]
            target[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                shutdown.validate(**args)
        for key, value in (("Pid", True), ("Pid", 0), ("Running", False), ("Status", "exited"), ("OOMKilled", True)):
            args = fixture(); args["before"]["State"][key] = value
            with self.subTest(before=key), self.assertRaises(ValueError):
                shutdown.validate(**args)

    def test_stale_unbounded_and_force_timeout_windows_rejected(self):
        for key, value in (("engine_until", SINCE), ("engine_until", "2026-09-06T07:20:56Z"),
                           ("engine_since", START), ("stop_elapsed_ns", 30 * 10**9),
                           ("stop_elapsed_ns", 0), ("stop_elapsed_ns", True)):
            args = fixture(); args[key] = value
            if key == "engine_since":
                # A timestamp after the chosen lower window must not be treated
                # as this process's already-running incarnation.
                args["before"]["State"]["StartedAt"] = SINCE
                args["after"]["State"]["StartedAt"] = SINCE
            with self.subTest(key=key), self.assertRaises(ValueError):
                shutdown.validate(**args)

    def test_missing_duplicate_extra_or_forced_event_rejected(self):
        original = records()
        values = [original[:2], original + [original[0]], original[:2] + [original[0]]]
        for index, key, value in ((0, "action", "oom"), (0, "id", "foreign"), (0, "scope", "swarm"),
                                  (1, "type", "network"), (1, "time_nano", True), (1, "time_nano", 1)):
            changed = copy.deepcopy(original); changed[index][key] = value; values.append(changed)
        for index, key, value in ((0, "signal", "9"), (0, "signal", "2"), (0, "signal", 15),
                                  (1, "exitCode", "0"), (1, "exitCode", "137"), (1, "execDuration", "-1"),
                                  (2, "name", "foreign"), (2, shutdown.LABEL, "foreign"), (2, "image", "foreign")):
            changed = copy.deepcopy(original); changed[index]["attributes"][key] = value; values.append(changed)
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                shutdown.validate(**dict(fixture(), event_bytes=encode(value)))

    def test_malformed_duplicate_key_or_truncated_raw_events_rejected(self):
        raw = encode(records())
        for value in (b"", raw[:-1], b"ERROR\n" + raw, raw + b"\n", raw.replace(b'"type": "container"',
                      b'"type": "container", "type": "container"', 1), b"x" * (shutdown.MAX_RAW + 1)):
            with self.subTest(value=value[:50]), self.assertRaises(ValueError):
                shutdown.validate(**dict(fixture(), event_bytes=value))

    def test_unrelated_forced_partial_duplicate_or_injected_log_rejected(self):
        values = [b"", ACTUAL_LOG[:-1], ACTUAL_LOG + ACTUAL_LOG, ACTUAL_LOG.split(b"\n", 1)[1],
                  ACTUAL_LOG.replace(b"got 1", b"got 3"), ACTUAL_LOG.replace(b"got 1", b"got 2"),
                  ACTUAL_LOG.replace(b":38", b":39"), ACTUAL_LOG.replace(b"runtime.goexit", b"other error"),
                  ACTUAL_LOG.replace(b".281221153", b".200000000"),
                  ACTUAL_LOG.replace(b"07:18:56", b"07:18:55"),
                  ACTUAL_LOG.replace(b"level=info", b"level=error"),
                  ACTUAL_LOG + b"2026-09-06T07:18:56.4Z closer failed\n",
                  ACTUAL_LOG.replace(b"runtime.goexit", b"\x1b[1mruntime.goexit"), b"x" * (shutdown.MAX_RAW + 1)]
        for value in values:
            with self.subTest(value=value[:50]), self.assertRaises(ValueError):
                shutdown.validate(**dict(fixture(), log_stderr=value))
        with self.assertRaises(ValueError):
            shutdown.validate(**dict(fixture(), log_stdout=ACTUAL_LOG))


if __name__ == "__main__":
    unittest.main()
