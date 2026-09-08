"""Offline mounts-suite adversaries; no Docker dispatch and no Machine."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import linux_docker_mounts_machine as subject

TOKEN = 'vzmounts-' + 'a' * 24
IMAGE = 'sha256:' + 'b' * 64


def line(key, value, owner=TOKEN):
    return 'VZMOUNT ' + owner + ' ' + key + '=' + value + '\n'


def report(case, **fields):
    rows = line('case', case) + line('uid', '0') + line('gid', '0')
    rows += ''.join(line(key, value) for key, value in fields.items())
    return (rows + line('end', case)).encode('ascii')


class FixtureTests(unittest.TestCase):
    def test_pinned_probe_and_contract_agree_with_every_module_constant(self):
        contract = subject.fixture_contract()
        self.assertEqual(contract['script_sha256'], subject.PROBE_SHA256)
        self.assertEqual(contract['contract']['probe']['cases'], sorted(subject.CASES))
        self.assertEqual(contract['contract']['bind']['host_to_container_sha256'], subject.INPUT_SHA256)
        self.assertEqual(subject.sha256(subject.INPUT), subject.INPUT_SHA256)
        self.assertEqual(subject.sha256(subject.OUTPUT), subject.OUTPUT_SHA256)
        self.assertEqual(subject.OUTPUT, (subject.INPUT_SHA256 + '\n').encode())

    def test_changed_probe_or_contract_bytes_are_refused(self, ):
        root = Path(subject.FIXTURE)
        import tempfile, shutil
        for name in ('probe.sh', 'fixture.json'):
            with tempfile.TemporaryDirectory() as temporary:
                copy_root = Path(temporary) / 'fixture'
                shutil.copytree(root, copy_root)
                (copy_root / name).write_bytes((copy_root / name).read_bytes() + b'\n# drift\n')
                with self.subTest(name=name), self.assertRaises(Exception):
                    subject.fixture_contract(copy_root)


class ReportTests(unittest.TestCase):
    def test_probe_lines_must_be_owned_named_and_complete(self):
        rows = subject.report(report('bind', input_sha256=subject.INPUT_SHA256), b'', TOKEN, 'bind')
        self.assertEqual(rows['input_sha256'], subject.INPUT_SHA256)
        for raw, reason in (
                (report('bind').replace(TOKEN.encode(), b'vzmounts-' + b'f' * 24), 'foreign owner'),
                (report('tmpfs_write'), 'another case'),
                (report('bind')[:-1], 'unterminated'),
                (report('bind') + line('case', 'bind').encode(), 'duplicate key'),
                (b'', 'empty')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.report(raw, b'', TOKEN, 'bind')
        with self.assertRaises(Exception):
            subject.report(report('bind'), b'warning\n', TOKEN, 'bind')


class RecipeTests(unittest.TestCase):
    """Each recipe decides the outcome; the probe only reports what it saw."""

    def session(self, reports):
        item = object.__new__(subject.Session)
        item.token = TOKEN
        item.root = '/run/vz-mounts-' + TOKEN
        item.volume_name = TOKEN + '-state'
        item.names, item.ids = {}, {}
        item.volume_created = False
        item.calls = []
        def run(role, case, args, *, extra=(), timeout=subject.RUN_TIMEOUT):
            item.calls.append((role, case, list(args), list(extra)))
            return reports[case]
        item.run = run
        item.read_host_output = lambda: {'output_sha256': subject.OUTPUT_SHA256, 'output_bytes': len(subject.OUTPUT)}
        item.create_volume = lambda: item.volume_name
        item.inspect_volume = lambda: {'name': item.volume_name, 'driver': 'local', 'scope': 'local',
                                       'labels': {subject.LABEL: TOKEN}, 'mountpoint': '/var/lib/docker/volumes'}
        item.volume_inventory = lambda label: [item.volume_name]
        return item

    def bind_reports(self, **changes):
        rows = {'input_sha256': subject.INPUT_SHA256, 'input_bytes': str(len(subject.INPUT)),
                'output_sha256': subject.OUTPUT_SHA256, 'undeclared': 'absent', 'workspace_source_count': '1'}
        rows.update(changes)
        return {'bind': dict(rows, case='bind', uid='0', gid='0', end='bind')}

    def test_bind_mounts_requires_exact_bytes_and_no_undeclared_path(self):
        session = self.session(self.bind_reports())
        proof = subject.bind_mounts(session, {'root': session.root})
        self.assertTrue(proof['only_declared_workspace_visible'])
        self.assertEqual(proof['host_to_container_sha256'], subject.INPUT_SHA256)
        self.assertIn('--mount', session.calls[0][2])
        for change, reason in ((('input_sha256', 'c' * 64), 'other bytes'),
                               (('output_sha256', 'c' * 64), 'other output'),
                               (('undeclared', 'present'), 'undeclared visible'),
                               (('workspace_source_count', '2'), 'two sources')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.bind_mounts(self.session(self.bind_reports(**dict([change]))), {'root': '/x'})

    def test_named_volume_must_return_the_first_container_bytes(self):
        digest = subject.sha256(TOKEN.encode())
        good = {'volume_write': {'payload_sha256': digest},
                'volume_read': {'payload_sha256': digest, 'payload_bytes': str(len(TOKEN))}}
        proof = subject.named_volumes(self.session(good))
        self.assertTrue(proof['same_machine_reuse_persists'])
        self.assertEqual(proof['volume_identity'], 'owned_named_volume')
        for change, reason in (({'volume_read': {'payload_sha256': 'c' * 64, 'payload_bytes': str(len(TOKEN))}}, 'other bytes'),
                               ({'volume_read': {'payload_sha256': digest, 'payload_bytes': '1'}}, 'other length'),
                               ({'volume_write': {'payload_sha256': 'c' * 64}}, 'unowned write')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.named_volumes(self.session(dict(good, **change)))

    def test_tmpfs_must_be_tmpfs_and_must_not_survive(self):
        digest = subject.sha256(TOKEN.encode())
        good = {'tmpfs_write': {'scratch_fstype': subject.TMPFS_MAGIC, 'payload_sha256': digest},
                'tmpfs_recreate': {'scratch_fstype': subject.TMPFS_MAGIC, 'payload': 'absent'}}
        self.assertTrue(subject.tmpfs(self.session(good))['data_absent_after_recreate'])
        for change, reason in (({'tmpfs_write': {'scratch_fstype': '58465342', 'payload_sha256': digest}}, 'not tmpfs'),
                               ({'tmpfs_recreate': {'scratch_fstype': subject.TMPFS_MAGIC, 'payload': 'present'}}, 'survived')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.tmpfs(self.session(dict(good, **change)))

    def test_read_only_rejects_an_accepted_write_and_a_refused_declared_write(self):
        good = {'readonly': {'root_write': 'refused', 'readonly_mount_write': 'refused',
                             'declared_writable_mount_write': 'written',
                             'readonly_source_sha256': subject.INPUT_SHA256}}
        proof = subject.read_only_mounts(self.session(good))
        self.assertEqual(proof, {'root_write': 'reject', 'readonly_mount_write': 'reject',
                                 'declared_writable_mount_write': 'success'})
        for key, value in (('root_write', 'written'), ('readonly_mount_write', 'written'),
                           ('declared_writable_mount_write', 'refused'), ('readonly_source_sha256', 'c' * 64)):
            with self.subTest(key=key), self.assertRaises(Exception):
                subject.read_only_mounts(self.session({'readonly': dict(good['readonly'], **{key: value})}))

    def test_ownership_requires_the_requested_identity_on_process_and_file(self):
        good = {'ownership': {'uid': '10001', 'gid': '10001', 'declared_writable_mount_write': 'written',
                              'created_uid': '10001', 'created_gid': '10001', 'forbidden_write': 'refused'}}
        proof = subject.ownership(self.session(good))
        self.assertEqual(proof['uid_gid'], '10001:10001')
        self.assertEqual(proof['forbidden_write'], 'reject')
        for key, value in (('uid', '0'), ('gid', '0'), ('created_uid', '0'), ('created_gid', '0'),
                           ('forbidden_write', 'written'), ('declared_writable_mount_write', 'refused')):
            with self.subTest(key=key), self.assertRaises(Exception):
                subject.ownership(self.session({'ownership': dict(good['ownership'], **{key: value})}))


class CrossMachineTests(unittest.TestCase):
    def observation(self, name, inventory):
        return {'named_volumes': {'volume': {'name': name}, 'inventory': list(inventory)}}

    def test_each_machine_sees_only_its_own_owned_volume(self):
        rows = [self.observation('a-state', ['a-state', 'unrelated']),
                self.observation('b-state', ['b-state']),
                self.observation('c-state', ['c-state', 'other'])]
        proof = subject.verify_machines(rows)
        self.assertTrue(proof['other_machine_cannot_read'])
        self.assertEqual(proof['machines'], 3)
        self.assertFalse(proof['full_storage_isolation_certified'])
        self.assertEqual(proof['owned_volumes'], ['a-state', 'b-state', 'c-state'])

    def test_shared_visible_or_missing_volume_is_rejected(self):
        for rows, reason in (
                ([self.observation('a-state', ['a-state', 'b-state']),
                  self.observation('b-state', ['b-state'])], 'foreign volume visible'),
                ([self.observation('a-state', ['unrelated']),
                  self.observation('b-state', ['b-state'])], 'own volume missing'),
                ([self.observation('a-state', ['a-state']),
                  self.observation('a-state', ['a-state'])], 'duplicate owned name'),
                ([self.observation('a-state', ['a-state'])], 'single Machine')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.verify_machines(rows)


class CoverageTests(unittest.TestCase):
    def test_the_five_storage_ids_are_claimed_and_no_longer_uncovered(self):
        import linux_docker_scenarios as scenarios
        claimed = [claim.id for claim in scenarios.claims('mounts')]
        self.assertEqual(claimed, ['docker.storage.bind_mounts', 'docker.storage.named_volumes',
                                   'docker.storage.tmpfs', 'docker.storage.read_only_mounts',
                                   'docker.storage.ownership'])
        uncovered = {identifier for identifier, _ in scenarios.UNCOVERED}
        self.assertFalse(uncovered & set(claimed))
        self.assertNotIn('mounts', scenarios.GAP_SUITES)
        self.assertEqual(scenarios.SUITES['mounts'].suite_evidence, ('mounts-cross-machine.json',))


if __name__ == '__main__':
    unittest.main()
