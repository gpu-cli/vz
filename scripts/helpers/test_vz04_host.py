import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vz04_host as host  # noqa: E402
import vz04_schema as schema  # noqa: E402

LSOF_SAMPLE = b"""p607
crapportd
f10
PTCP
n*:50020
TST=LISTEN
TQR=0
f21
PUDP
n*:3722
p629
cidentityservicesd
f7
PUDP
n*:*
p4242
cvz-runtimed
f5
PTCP
n127.0.0.1:41000
TST=LISTEN
f6
PTCP
n127.0.0.1:41001->127.0.0.1:50000
TST=ESTABLISHED
f7
PTCP
n[::1]:41002
TST=LISTEN
"""

NETSTAT_SAMPLE = b"""Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp6       0      0  *.53                   *.*                    LISTEN
tcp4       0      0  127.0.0.1.41000        *.*                    LISTEN
tcp4       0      0  192.168.1.10.49152     1.2.3.4.443            ESTABLISHED
udp4       0      0  *.3722                 *.*
udp4       0      0  192.168.1.10.5353      *.*
udp4       0      0  127.0.0.1.60000        127.0.0.1.60001
"""

PS_SAMPLE = b"""    1     0 /sbin/launchd
  100     1 /bin/zsh
  200   100 uv run vz04_gate.py --run-id gate-test-run-1
  300   200 python -B vz04_gate.py --run-id gate-test-run-1
  400     1 /tmp/state/bin/vz-runtimed --state-root /tmp/state
  500     1 /opt/release/bin/vz status
  600     1 /usr/bin/true
"""


class ParserTests(unittest.TestCase):
    def test_classify_address(self):
        self.assertEqual(host.classify_address("*"), "wildcard")
        self.assertEqual(host.classify_address("0.0.0.0"), "wildcard")
        self.assertEqual(host.classify_address("[::]"), "wildcard")
        self.assertEqual(host.classify_address("127.0.0.1"), "loopback")
        self.assertEqual(host.classify_address("[::1]"), "loopback")
        self.assertEqual(host.classify_address("::1"), "loopback")
        self.assertEqual(host.classify_address("192.168.1.10"), "non_loopback")
        self.assertEqual(host.classify_address("fe80::1%en0"), "non_loopback")

    def test_parse_lsof_keeps_listen_and_bound_udp_only(self):
        rows = host.parse_lsof(LSOF_SAMPLE)
        self.assertEqual([(r["protocol"], r["address"], r["port"], r["pid"], r["command"], r["scope"]) for r in rows],
                         [("tcp", "*", 50020, 607, "rapportd", "wildcard"), ("udp", "*", 3722, 607, "rapportd", "wildcard"),
                          ("tcp", "127.0.0.1", 41000, 4242, "vz-runtimed", "loopback"), ("tcp", "[::1]", 41002, 4242, "vz-runtimed", "loopback")])

    def test_parse_netstat(self):
        rows = host.parse_netstat(NETSTAT_SAMPLE)
        self.assertEqual([(r["protocol"], r["address"], r["port"], r["scope"]) for r in rows],
                         [("tcp", "*", 53, "wildcard"), ("tcp", "127.0.0.1", 41000, "loopback"), ("udp", "*", 3722, "wildcard"),
                          ("udp", "192.168.1.10", 5353, "non_loopback")])

    def test_merge_marks_sources(self):
        merged = host.merge_listeners(host.parse_lsof(LSOF_SAMPLE), host.parse_netstat(NETSTAT_SAMPLE))
        by_key = {host.listener_key(r): r for r in merged}
        self.assertEqual(by_key[("tcp", "127.0.0.1", 41000)]["source"], "both")
        self.assertEqual(by_key[("tcp", "127.0.0.1", 41000)]["pid"], 4242)
        self.assertEqual(by_key[("tcp", "*", 53)]["source"], "netstat")
        self.assertIsNone(by_key[("tcp", "*", 53)]["pid"])
        self.assertEqual(by_key[("tcp", "[::1]", 41002)]["source"], "lsof")

    def test_scoped_processes_exclude_ancestry(self):
        table = host.parse_ps(PS_SAMPLE)
        needles = [("run_id", "gate-test-run-1"), ("state_root", "/tmp/state"), ("release_bin", "/opt/release/bin")]
        rows = host.scoped_processes(table, needles, excluded={200, 300})
        self.assertEqual([(r["pid"], r["ppid"], r["matched"]) for r in rows], [(400, 1, ["state_root"]), (500, 1, ["release_bin"])])
        rows = host.scoped_processes(table, needles, excluded=set())
        self.assertEqual([r["pid"] for r in rows], [200, 300, 400, 500])

    def test_own_ancestry_contains_self_and_parent(self):
        row, data = host.run_capture([host.PS, "-axo", "pid=,ppid=,command="])
        table = host.parse_ps(data)
        ancestry = host.own_ancestry(table)
        self.assertIn(os.getpid(), ancestry)
        self.assertIn(os.getppid(), ancestry)
        self.assertIn(1, ancestry)

    def test_unix_sockets_under_state_root(self):
        with tempfile.TemporaryDirectory(prefix="vz04-host-") as tmp:
            root = Path(tmp).resolve()
            (root / "sub").mkdir()
            (root / "sub" / "plain.txt").write_text("x")
            path = root / "sub" / "d.sock"
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                server.bind(str(path))
                self.assertEqual(host.unix_sockets(root), [str(path)])
            finally:
                server.close()
        self.assertEqual(host.unix_sockets(Path("/nonexistent-vz04")), [])


class InventoryAndDiffTests(unittest.TestCase):
    def inventory(self, moment, **overrides):
        value = {"schema_version": 1, "kind": "vz-0.4-host-inventory", "run_id": "gate-test-run-1", "moment": moment,
                 "captured_at_utc": "2026-09-07T00:00:00Z", "capture_state": "captured", "capture_errors": [],
                 "state_root": "/tmp/state", "docker_config": "/tmp/state/docker",
                 "sources": {name: {"argv": [name], "exit_code": 0, "error": None, "stdout_sha256": "0" * 64, "truncated": False}
                             for name in ("lsof", "netstat", "ps", "docker_context_ls")},
                 "listeners": host.merge_listeners(host.parse_lsof(LSOF_SAMPLE), host.parse_netstat(NETSTAT_SAMPLE)),
                 "processes": [], "excluded_pids": [1], "process_count": 7, "sockets": [],
                 "docker_contexts": [{"name": "default", "endpoint": "unix:///var/run/docker.sock"}]}
        value.update(overrides)
        return value

    def test_capture_on_this_host_is_schema_valid(self):
        with tempfile.TemporaryDirectory(prefix="vz04-host-") as tmp:
            root = Path(tmp).resolve()
            scope = host.HostScope(run_id="gate-test-run-1", state_root=root, release_dir=root / "release",
                                   clients={"docker": None, "compose_plugin": None, "buildx_plugin": None})
            value = host.capture(scope, "before")
            self.assertEqual(schema.validate("host-inventory", value), [])
            self.assertEqual(value["capture_state"], "captured", value["capture_errors"])
            self.assertTrue(value["listeners"])
            self.assertIn(os.getpid(), value["excluded_pids"])
            self.assertEqual(value["sockets"], [])
            self.assertEqual(value["docker_contexts"], [])

    def test_diff_clean(self):
        before, after = self.inventory("before"), self.inventory("after")
        diff = host.diff(before, after, {"wildcard_listeners_forbidden": True})
        self.assertEqual(schema.validate("leak-diff", diff), [])
        self.assertEqual(diff["survivors"], [])
        self.assertEqual(diff["new_listeners"], [])

    def test_diff_reports_survivors(self):
        before = self.inventory("before")
        after = self.inventory("after",
                               processes=[{"pid": 400, "ppid": 1, "command": "/tmp/state/bin/vz-runtimed", "matched": ["state_root"]}],
                               sockets=["/tmp/state/d.sock"],
                               docker_contexts=[{"name": "default", "endpoint": None}, {"name": "vz-gate-test-run-1", "endpoint": "unix:///tmp/state/x"}])
        after["listeners"] = after["listeners"] + [
            {"protocol": "tcp", "address": "127.0.0.1", "port": 41999, "pid": 400, "command": "vz-runtimed", "scope": "loopback", "source": "lsof"},
            {"protocol": "tcp", "address": "*", "port": 8080, "pid": 999, "command": "node", "scope": "wildcard", "source": "lsof"},
            {"protocol": "tcp", "address": "127.0.0.1", "port": 9999, "pid": 998, "command": "Safari", "scope": "loopback", "source": "lsof"},
            {"protocol": "udp", "address": "*", "port": 5353, "pid": None, "command": None, "scope": "wildcard", "source": "netstat"}]
        diff = host.diff(before, after, {"wildcard_listeners_forbidden": True})
        self.assertEqual(schema.validate("leak-diff", diff), [])
        kinds = [s.split(" ", 1)[0] for s in diff["survivors"]]
        self.assertEqual(sorted(kinds), ["docker_context", "listener", "listener", "listener", "process", "socket"])
        self.assertTrue(any("41999" in s for s in diff["survivors"]))
        self.assertTrue(any("8080" in s for s in diff["survivors"]))
        self.assertTrue(any("5353" in s for s in diff["survivors"]))
        self.assertFalse(any("9999" in s for s in diff["survivors"]))
        self.assertEqual(diff["unrelated_new_listener_count"], 1)
        self.assertEqual(len(diff["new_listeners"]), 4)


if __name__ == "__main__":
    unittest.main()
