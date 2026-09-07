from datetime import datetime, timedelta, timezone
import io
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vz04_common as common  # noqa: E402
import vz04_schema as schema  # noqa: E402
import vz04_sleepwake as sleepwake  # noqa: E402

PMSET_SAMPLE = b"""Total Sleep/Wakes since boot at 2026-08-30 15:27:54 -0600 :7
UUID: FA7871ED-AC11-4B05-AF48-61F0C948AEAD
2026-09-01 08:09:35 -0600 Sleep               \tEntering Sleep state due to 'Low Power Sleep':TCPKeepAlive=inactive Using Batt (Charge:1%) 2759 secs
2026-09-01 08:09:37 -0600 Wake Requests       \t[*process=dasd request=SleepService deltaSecs=954 wakeAt=2026-09-01 08:25:31]
2026-09-01 08:55:34 -0600 Wake                \tWake from Hibernate [CDNVA] : due to acattach/UserActivity Assertion Using AC (Charge:1%)
2026-09-06 20:47:10 -0600 Assertions          \tPID 626(Microsoft Edge) Created NoDisplaySleepAssertion "Video Wake Lock" 00:00:00
2026-09-06 21:00:00 -0600 DarkWake            \tDarkWake from Deep Idle [CDNVA] : due to maintenance
"""

NONCE = "ab" * 32


def record(observed=True, **wake_overrides):
    checkpoint = {"nonce": NONCE, "boot_session_uuid": "BOOT-1", "boottime_unix": 1788125274, "clock_monotonic_ns": 1_000_000_000_000,
                  "clock_uptime_raw_ns": 900_000_000_000, "wall_clock_utc": "2026-09-01T14:00:00Z", "pmset_log_sha256": "1" * 64}
    capture = dict(checkpoint, clock_monotonic_ns=1_000_000_000_000 + 3_000_000_000_000, clock_uptime_raw_ns=900_000_000_000 + 100_000_000_000,
                   wall_clock_utc="2026-09-01T15:00:00Z", pmset_log_sha256="2" * 64)
    wake = {"capture": capture, "delta_monotonic_ns": 3_000_000_000_000, "delta_uptime_raw_ns": 100_000_000_000,
            "discontinuity_ns": 2_900_000_000_000, "discontinuity_seconds": 2900.0, "nonce_echoed": True, "same_boot_session": True,
            "power_events": [{"source": "pmset -g log", "timestamp_utc": "2026-09-01T14:09:35Z", "event": "Sleep: Entering Sleep state"},
                             {"source": "pmset -g log", "timestamp_utc": "2026-09-01T14:55:34Z", "event": "Wake: Wake from Hibernate"}],
            "unified_log": {"argv": ["/usr/bin/log", "show"], "state": "captured", "bytes": 10, "message_count": 1, "error": None, "elapsed_seconds": 1.0}}
    wake.update(wake_overrides)
    return {"schema_version": 1, "kind": "vz-0.4-sleep-wake", "run_id": "gate-test-run-1", "minimum_sleep_seconds": 20,
            "observed": observed, "reason": None if observed else "power_events_missing", "checkpoint": checkpoint,
            "wake": wake if observed else None,
            "ack": {"state": "acked", "channel": "file", "waited_seconds": 12.5, "detail": "/tmp/ack"}}


class ClockTests(unittest.TestCase):
    def test_both_clocks_available_and_coherent(self):
        first = sleepwake.clocks()
        second = sleepwake.clocks()
        self.assertGreaterEqual(second["clock_monotonic_ns"], first["clock_monotonic_ns"])
        self.assertGreaterEqual(second["clock_uptime_raw_ns"], first["clock_uptime_raw_ns"])
        discontinuity = (second["clock_monotonic_ns"] - first["clock_monotonic_ns"]) - (second["clock_uptime_raw_ns"] - first["clock_uptime_raw_ns"])
        self.assertLess(abs(discontinuity), 1_000_000_000, "no sleep happened between two immediate reads")
        self.assertNotEqual(first["boot_session_uuid"], "unavailable")
        self.assertGreater(first["boottime_unix"], 0)

    def test_pmset_log_readable_unprivileged(self):
        row, data = sleepwake.pmset_log()
        self.assertEqual(row["exit_code"], 0, row["error"])
        self.assertTrue(sleepwake.parse_pmset_rows(data))


class ParserTests(unittest.TestCase):
    def test_parse_pmset_rows_and_domains(self):
        rows = sleepwake.parse_pmset_rows(PMSET_SAMPLE)
        self.assertEqual([r["domain"] for r in rows], ["Sleep", "Wake Requests", "Wake", "Assertions", "DarkWake"])
        self.assertEqual(rows[0]["timestamp_utc"], "2026-09-01T14:09:35Z")

    def test_power_events_filtered_to_window_and_domains(self):
        start, end = datetime(2026, 9, 1, 14, 0, tzinfo=timezone.utc), datetime(2026, 9, 1, 15, 0, tzinfo=timezone.utc)
        events = sleepwake.pmset_power_events(PMSET_SAMPLE, start, end)
        self.assertEqual([e["event"].split(":", 1)[0] for e in events], ["Sleep", "Wake"])
        self.assertTrue(sleepwake.paired_sleep_wake(events))
        only_wake = sleepwake.pmset_power_events(PMSET_SAMPLE, start + timedelta(minutes=30), end)
        self.assertFalse(sleepwake.paired_sleep_wake(only_wake))
        self.assertEqual(sleepwake.pmset_power_events(PMSET_SAMPLE, end, end + timedelta(hours=1)), [])

    def test_unified_log_bounded_and_explicit_on_failure(self):
        end = datetime.now(timezone.utc)
        status, events = sleepwake.unified_log_events(end - timedelta(seconds=5), end, timeout=60, log_binary="/nonexistent/log")
        self.assertEqual(status["state"], "unavailable")
        self.assertEqual(events, [])
        self.assertIn("nonexistent", status["error"])
        status, events = sleepwake.unified_log_events(end - timedelta(seconds=5), end, timeout=0, log_binary="/bin/sleep")
        self.assertEqual(status["state"], "timeout")

    def test_unified_log_show_on_this_host(self):
        end = datetime.now(timezone.utc)
        status, events = sleepwake.unified_log_events(end - timedelta(seconds=20), end, timeout=120)
        self.assertEqual(status["state"], "captured", status["error"])
        self.assertLess(status["elapsed_seconds"], 120)
        for event in events:
            self.assertRegex(event["timestamp_utc"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class VerifyTests(unittest.TestCase):
    def test_bound_record_has_no_findings_and_is_schema_valid(self):
        value = record()
        self.assertEqual(schema.validate("sleep-wake", value), [])
        self.assertEqual(sleepwake.verify(value, 20), [])

    def test_not_observed(self):
        value = record(observed=False)
        self.assertEqual(schema.validate("sleep-wake", value), [])
        self.assertEqual([c for c, _s, _d in sleepwake.verify(value, 20)], ["sleep_wake.not_observed"])

    def test_each_binding_failure_is_reported(self):
        codes = lambda v: [c for c, _s, _d in sleepwake.verify(v, 20)]  # noqa: E731
        self.assertEqual(codes(record(same_boot_session=False)), ["sleep_wake.boot_session"])
        self.assertEqual(codes(record(nonce_echoed=False)), ["sleep_wake.nonce"])
        self.assertEqual(codes(record(discontinuity_seconds=19.9)), ["sleep_wake.short"])
        self.assertEqual(codes(record(power_events=[])), ["sleep_wake.power_events"])
        self.assertEqual(codes(record(power_events=[{"source": "pmset -g log", "timestamp_utc": "2026-09-01T14:55:34Z", "event": "Wake: x"}])),
                         ["sleep_wake.power_events"])
        outside = record()
        outside["wake"]["power_events"].append({"source": "pmset -g log", "timestamp_utc": "2026-09-01T16:00:00Z", "event": "Wake: late"})
        self.assertIn("sleep_wake.window", codes(outside))
        self.assertEqual(codes(record(unified_log={"argv": [], "state": "timeout", "bytes": 0, "message_count": 0, "error": "slow", "elapsed_seconds": 300.0})),
                         ["sleep_wake.unified_log"])
        changed = record()
        changed["wake"]["capture"]["boot_session_uuid"] = "BOOT-2"
        self.assertEqual(codes(changed), ["sleep_wake.boot_session"])


class AckTests(unittest.TestCase):
    def test_ack_file_with_nonce(self):
        with tempfile.TemporaryDirectory(prefix="vz04-ack-") as tmp:
            ack = Path(tmp) / "ack"
            threading.Timer(0.6, lambda: ack.write_text("wake done " + NONCE + "\n")).start()
            result = sleepwake.wait_for_ack(NONCE, 30, str(ack), prompt_stream=io.StringIO())
            self.assertEqual((result["state"], result["channel"]), ("acked", "file"))
            self.assertGreaterEqual(result["waited_seconds"], 0.5)

    def test_ack_file_without_nonce_is_mismatch(self):
        with tempfile.TemporaryDirectory(prefix="vz04-ack-") as tmp:
            ack = Path(tmp) / "ack"
            ack.write_text("done\n")
            result = sleepwake.wait_for_ack(NONCE, 30, str(ack), prompt_stream=io.StringIO())
            self.assertEqual(result["state"], "nonce_mismatch")

    def test_deadline_exceeded(self):
        if os.isatty(0):
            self.skipTest("controlling TTY present; deadline test needs a non-interactive stdin")
        with tempfile.TemporaryDirectory(prefix="vz04-ack-") as tmp:
            result = sleepwake.wait_for_ack(NONCE, 0, str(Path(tmp) / "never"), prompt_stream=io.StringIO(), sleep=lambda _s: None,
                                            clock=iter([0.0, 0.0, 5.0]).__next__)
            self.assertEqual(result["state"], "ack_deadline_exceeded")

    def test_no_channel(self):
        if os.isatty(0):
            self.skipTest("controlling TTY present")
        result = sleepwake.wait_for_ack(NONCE, 30, None, prompt_stream=io.StringIO())
        self.assertEqual(result["state"], "operator_ack_missing")


class ObserveTests(unittest.TestCase):
    def test_dry_writes_checkpoint_without_waiting(self):
        contract = {"sleep_wake": {"minimum_sleep_seconds": 20}, "deadlines_seconds": {"operator_ack": 1800}}
        with tempfile.TemporaryDirectory(prefix="vz04-sw-") as tmp:
            root = Path(tmp).resolve()
            started = time.monotonic()
            path = sleepwake.observe(root, "gate-test-run-1", contract, ack_file=None, dry=True, prompt_stream=io.StringIO())
            self.assertLess(time.monotonic() - started, 60)
            value = common.load_json(path)
            self.assertEqual(schema.validate("sleep-wake", value), [])
            self.assertEqual((value["observed"], value["reason"], value["ack"]["state"]), (False, "dry_lanes", "not_attempted"))
            stored = common.load_json(sleepwake.checkpoint_path(root))
            self.assertEqual(schema.validate("sleep-wake-checkpoint", stored), [])
            self.assertEqual(stored["checkpoint"], value["checkpoint"])
            self.assertEqual([c for c, _s, _d in sleepwake.verify(value, 20)], ["sleep_wake.not_observed"])

    def test_real_ack_without_sleep_is_discontinuity_too_small(self):
        if os.isatty(0):
            self.skipTest("controlling TTY present; would block on Enter")
        contract = {"sleep_wake": {"minimum_sleep_seconds": 20}, "deadlines_seconds": {"operator_ack": 60}}
        with tempfile.TemporaryDirectory(prefix="vz04-sw-") as tmp:
            root = Path(tmp).resolve()
            ack = root / "ack"

            def acknowledge():
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline and not sleepwake.checkpoint_path(root).exists():
                    time.sleep(0.05)
                stored = common.load_json(sleepwake.checkpoint_path(root))
                ack.write_text(stored["checkpoint"]["nonce"])

            threading.Thread(target=acknowledge, daemon=True).start()
            path = sleepwake.observe(root, "gate-test-run-1", contract, ack_file=str(ack), dry=False, prompt_stream=io.StringIO())
            value = common.load_json(path)
            self.assertEqual(schema.validate("sleep-wake", value), [])
            self.assertFalse(value["observed"])
            self.assertIn(value["reason"], ("discontinuity_too_small", "power_events_missing"))
            self.assertEqual(value["ack"]["state"], "acked")
            self.assertTrue(value["wake"]["nonce_echoed"])
            self.assertTrue(value["wake"]["same_boot_session"])
            self.assertLess(value["wake"]["discontinuity_seconds"], 20)


if __name__ == "__main__":
    unittest.main()
