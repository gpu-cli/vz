"""Offline netpolicy adversaries; no Docker dispatch and no Machine."""
import json
import shutil
import tempfile
from pathlib import Path
import unittest

import linux_docker_netpolicy_machine as subject

TOKEN = 'vznet-' + 'a' * 24


def table(*rows):
    head = 'Active Internet connections (only servers)\nProto Recv-Q Send-Q Local Address Foreign Address State\n'
    return (head + ''.join('tcp 0 0 %s:%s 0.0.0.0:* LISTEN\n' % row for row in rows)).encode('ascii')


class FixtureTests(unittest.TestCase):
    def test_pinned_probe_and_contract_agree_with_every_module_constant(self):
        contract = subject.fixture_contract()
        self.assertEqual(contract['script_sha256'], subject.PROBE_SHA256)
        self.assertEqual(contract['contract']['probe']['container_port'], subject.CONTAINER_PORT)
        self.assertEqual(contract['contract']['published_ports']['roles'], list(subject.ROLES))
        self.assertEqual(subject.response_bytes(TOKEN, 'alpha'), ('vznet|' + TOKEN + '|alpha').encode())

    def test_changed_probe_or_contract_bytes_are_refused(self):
        for name in ('probe.sh', 'fixture.json'):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / 'fixture'
                shutil.copytree(subject.FIXTURE, root)
                (root / name).write_bytes((root / name).read_bytes() + b'\n# drift\n')
                with self.subTest(name=name), self.assertRaises(Exception):
                    subject.fixture_contract(root)


class ListenerTests(unittest.TestCase):
    def test_only_owned_ports_are_parsed_and_the_address_is_preserved(self):
        raw = table(('127.0.0.1', 32768), ('0.0.0.0', 32769), ('192.168.1.9', 32770), ('127.0.0.1', 22))
        rows = subject.listener_rows(raw, [32768, 32769, 32770])
        self.assertEqual(rows, [{'address': '127.0.0.1', 'port': 32768},
                                {'address': '0.0.0.0', 'port': 32769},
                                {'address': '192.168.1.9', 'port': 32770}])
        # A port this run never published is none of the suite's business.
        self.assertEqual(subject.listener_rows(raw, [22]), [{'address': '127.0.0.1', 'port': 22}])
        self.assertEqual(subject.listener_rows(raw, []), [])

    def test_bracketed_ipv6_and_noise_lines(self):
        raw = ('Proto Local\ntcp6 0 0 [::]:32768 [::]:* LISTEN\nnonsense\nudp 0 0 127.0.0.1:32768 x x\n').encode()
        self.assertEqual(subject.listener_rows(raw, [32768]), [{'address': '::', 'port': 32768}])


class SessionStub:
    def __init__(self, listeners, *, responses=None, ports=None, digest='d' * 64):
        self.token = TOKEN
        self.network_id = 'e' * 64
        self.ports = ports if ports is not None else {'alpha': 32768, 'beta': 32769}
        self._listeners = listeners
        self._responses = responses or {role: subject.sha256(subject.response_bytes(TOKEN, role))
                                        for role in subject.ROLES}
        self.digest = digest
        self.removed = False
        self.routes = b'default via 10.0.0.1\n'
    def create_network(self):
        return self.network_id
    def serve(self, role):
        return role
    def published_port(self, role):
        return {'role': role, 'container_id': 'f' * 64, 'host_ip': subject.LOOPBACK,
                'host_port': self.ports[role], 'container_port': subject.CONTAINER_PORT}
    def fetch(self, role):
        return self._responses[role]
    def listeners(self, label, *, ports=None):
        return self._listeners if not self.removed else []
    def unrelated_digest(self, label):
        return {'networks': 2, 'sha256': self.digest}
    def public_exec(self, label, script, *, allow_failure=False):
        return self.routes, b'', 0
    def remove(self):
        self.removed = True


class PublishedPortTests(unittest.TestCase):
    def loopback(self, *ports):
        return [{'address': subject.LOOPBACK, 'port': port} for port in ports]

    def test_loopback_only_unique_ports_and_exact_responses(self):
        session = SessionStub(self.loopback(32768, 32769))
        proof = subject.published_ports(session)
        self.assertEqual(proof['listener_address'], '127.0.0.1')
        self.assertTrue(proof['assigned_ports_unique'])
        self.assertFalse(proof['wildcard_or_lan_listener'])
        self.assertEqual([row['host_port'] for row in proof['published']], [32768, 32769])

    def test_wildcard_lan_duplicate_or_missing_listener_rejected(self):
        for listeners, ports, reason in (
                ([{'address': '0.0.0.0', 'port': 32768}, {'address': subject.LOOPBACK, 'port': 32769}],
                 None, 'wildcard bind'),
                ([{'address': '192.168.1.9', 'port': 32768}, {'address': subject.LOOPBACK, 'port': 32769}],
                 None, 'LAN bind'),
                (self.loopback(32768), None, 'missing listener'),
                (self.loopback(32768, 32769, 32770), None, 'extra listener'),
                (self.loopback(32768, 32768), {'alpha': 32768, 'beta': 32768}, 'duplicate port'),
                (self.loopback(32768, 32771), None, 'listener on another port')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.published_ports(SessionStub(listeners, ports=ports))

    def test_a_foreign_response_body_is_rejected(self):
        session = SessionStub(self.loopback(32768, 32769),
                              responses={'alpha': 'c' * 64, 'beta': 'c' * 64})
        with self.assertRaisesRegex(Exception, 'identical bytes'):
            subject.published_ports(session)


class CleanupTests(unittest.TestCase):
    def test_removal_retires_every_owned_listener_route_and_leaves_others(self):
        session = SessionStub([{'address': subject.LOOPBACK, 'port': 32768}])
        proof = subject.network_cleanup(session, {'networks': 2, 'sha256': 'd' * 64})
        self.assertTrue(proof['owned_routes_dns_listeners_mounts_absent'])
        self.assertTrue(proof['unrelated_inventory_sha256_unchanged'])
        self.assertFalse(proof['full_host_listener_certification'])
        self.assertEqual(proof['retired_ports'], [32768, 32769])

    def test_surviving_listener_bridge_or_changed_unrelated_inventory_rejected(self):
        session = SessionStub([])
        session.remove = lambda: None  # a listener that outlives the removal
        session._listeners = [{'address': subject.LOOPBACK, 'port': 32768}]
        with self.assertRaisesRegex(Exception, 'listener survived'):
            subject.network_cleanup(session, {'networks': 2, 'sha256': 'd' * 64})
        session = SessionStub([])
        session.routes = ('7: br-' + 'e' * 12 + ': <BROADCAST>\n').encode()
        with self.assertRaisesRegex(Exception, 'bridge interface or route survived'):
            subject.network_cleanup(session, {'networks': 2, 'sha256': 'd' * 64})
        session = SessionStub([], digest='9' * 64)
        with self.assertRaisesRegex(Exception, 'unrelated network changed'):
            subject.network_cleanup(session, {'networks': 2, 'sha256': 'd' * 64})


class CoverageTests(unittest.TestCase):
    def test_both_network_ids_are_claimed_and_no_longer_uncovered(self):
        import linux_docker_scenarios as scenarios
        claimed = [claim.id for claim in scenarios.claims('netpolicy')]
        self.assertEqual(claimed, ['docker.network.published_ports', 'docker.network.cleanup'])
        self.assertFalse({identifier for identifier, _ in scenarios.UNCOVERED} & set(claimed))
        self.assertNotIn('netpolicy', scenarios.GAP_SUITES)
        rows = scenarios.manifest()
        self.assertEqual(rows['docker.network.cleanup']['phase'], 'final-cleanup')
        self.assertEqual(rows['docker.network.published_ports']['phase'], 'clean-provision')


if __name__ == '__main__':
    unittest.main()
