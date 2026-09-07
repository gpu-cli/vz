"""Mocked exact-resource/session boundaries; no Docker or guest dispatch."""
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_registry_commands as commands
import linux_docker_registry_fixture as fixture
import linux_docker_registry_session as subject

OWNER = {'project_id': 'project', 'environment_id': 'environment', 'machine_id': 'machine'}
SPEC = fixture.resource_spec(OWNER, 'run')


def network():
    return {'Name': SPEC['network_name'], 'Id': 'a' * 64, 'Driver': 'bridge', 'Scope': 'local',
            'Internal': True, 'Labels': dict(SPEC['labels']), 'EnableIPv6': False,
            'IPAM': {'Driver': 'default', 'Config': [{'Subnet': SPEC['subnet'], 'Gateway': SPEC['gateway']}]},
            'Containers': {}}


def volume():
    return {'Name': SPEC['volume_name'], 'Driver': 'local', 'Scope': 'local',
            'Labels': dict(SPEC['labels']), 'Options': None}


def encoded(row):
    return json.dumps([row]).encode()


class IdentityTests(unittest.TestCase):
    def test_exact_internal_network(self):
        row = network()
        self.assertEqual(subject.network_identity(encoded(row), SPEC), row)

    def test_network_rejects_foreign_and_public_topology(self):
        for key, value in (('Name', 'foreign'), ('Id', 'short'), ('Driver', 'host'),
                           ('Scope', 'swarm'), ('Internal', False), ('Labels', {}),
                           ('EnableIPv6', True), ('IPAM', {'Driver': 'default', 'Config': []})):
            row = network(); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                subject.network_identity(encoded(row), SPEC)

    def test_network_rejects_wrong_gateway_and_extra_ranges(self):
        row = network(); row['IPAM']['Config'][0]['Gateway'] = '172.30.241.9'
        with self.assertRaises(ValueError):
            subject.network_identity(encoded(row), SPEC)
        row = network(); row['IPAM']['Config'].append({'Subnet': '10.0.0.0/8'})
        with self.assertRaises(ValueError):
            subject.network_identity(encoded(row), SPEC)

    def test_exact_volume(self):
        row = volume()
        self.assertEqual(subject.volume_identity(encoded(row), SPEC), row)

    def test_volume_rejects_foreign_or_host_backed_options(self):
        for key, value in (('Name', 'foreign'), ('Driver', 'foreign'), ('Scope', 'swarm'),
                           ('Labels', {}), ('Options', {'device': '/foreign', 'type': 'none'})):
            row = volume(); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                subject.volume_identity(encoded(row), SPEC)

    def test_single_resource_inventory_required(self):
        for validate, row in ((subject.network_identity, network()), (subject.volume_identity, volume())):
            for raw in (b'[]', json.dumps([row, row]).encode(), b'{}'):
                with self.assertRaises(ValueError):
                    validate(raw, SPEC)


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.harness = SimpleNamespace(info={'run_id': 'run', 'registry_archive': '/owned/registry.tar',
            'registry': {'manifest_digest': 'sha256:' + 'b' * 64},
            'clients': {'docker': {'canonical': '/owned/docker'}}},
            evidence=Path('/owned/evidence'), config=Path('/owned/config'), cli=Path('/owned/vz'),
            record=SimpleNamespace(canaries=[], receipts=[]), sensitive_canaries=[], registry_sessions=[],
            effects_uncertain=False, monitor=None,
            assert_certain=Mock(), docker=Mock(return_value=(b'', b'', 0)),
            mutate=Mock(return_value=(b'', b'', 0)), exact_absent=Mock(), command=Mock())
        self.descriptor = {'owner': dict(OWNER), 'config_dir': '/owned/machine-config',
                           'name': 'exact-context', 'incarnation_id': 'incarnation'}
        self.private = Mock()
        self.private.password.return_value = b'1' * 48
        self.private.canaries.return_value = (b'1' * 48, b'2' * 48)
        self.private.public.return_value = {'owner': dict(OWNER), 'run_id': 'run',
                                           'authority': SPEC['authority']}
        self.private.privatefiles.return_value = {name: b'fixture-' + name.encode()
            for name in subject.guest.FILES}
        self.private.ca_pem.return_value = b'public-fixture-ca'
        self.private.pins = {'ca_sha256': 'c' * 64}
        self.owned_commands = Mock(spec=commands.Commands)
        self.owned_commands.assert_certain = Mock()
        self.store = Mock()
        self.store.snapshot.return_value = object()
        self.store.check_transition.return_value = {'specific_auth_transition': True}
        self.store.check_unchanged.return_value = {'unchanged': True}
        patches = (patch.object(commands, 'Commands', return_value=self.owned_commands),
                   patch.object(subject.credentials, 'Store', return_value=self.store),
                   patch.object(subject.startup, 'private', side_effect=lambda path: path),
                   patch.object(subject.startup, 'document'),
                   patch.object(subject.fixture, 'validate_tls_public'))
        self.mocks = []
        for item in patches:
            self.mocks.append(item.start())
            self.addCleanup(item.stop)

    def session(self):
        return subject.Session(self.harness, self.descriptor, '/owned/project', 0, self.private)

    def test_construction_registers_without_dispatch(self):
        item = self.session()
        self.assertEqual(self.harness.registry_sessions, [item])
        self.assertFalse(item.prepared)
        self.assertFalse(item.workload_complete)
        self.assertFalse(item.cleanup_complete)
        self.harness.docker.assert_not_called()
        self.harness.mutate.assert_not_called()
        self.owned_commands.private.assert_not_called()
        self.store.snapshot.assert_called_once_with(expected='empty')
        self.private.validate_private.assert_called_once()
        self.mocks[4].assert_called_once()

    def test_native_or_owner_admission_failure_precedes_side_effects(self):
        for admission in (self.private.validate_private, self.mocks[4]):
            admission.side_effect = ValueError('fixture rejected')
            with self.assertRaisesRegex(ValueError, 'fixture rejected'):
                self.session()
            self.mocks[0].assert_not_called()
            self.mocks[2].assert_not_called()
            self.harness.mutate.assert_not_called()
            self.assertEqual(self.harness.registry_sessions, [])
            admission.side_effect = None

    def test_certain_checks_private_owner_and_harness(self):
        item = self.session(); item.certain()
        self.owned_commands.assert_certain.assert_called_once()
        self.harness.assert_certain.assert_not_called()
        item.failed = True
        with self.assertRaises(ValueError):
            item.certain()

    def test_uncertain_main_receipt_fences_session(self):
        item = self.session()
        self.harness.record.receipts.append({'effects_uncertain': True})
        with self.assertRaises(ValueError):
            item.docker('must-not-run', ['version'])
        self.harness.docker.assert_not_called()

    def test_live_monitor_failure_fences_session(self):
        item = self.session()
        self.harness.monitor = SimpleNamespace(thread=SimpleNamespace(is_alive=lambda: True),
            check=Mock(side_effect=ValueError('sentinel failed')))
        with self.assertRaisesRegex(ValueError, 'sentinel failed'):
            item.docker('must-not-run', ['version'])
        self.harness.docker.assert_not_called()

    def test_exact_public_exec_argv_and_private_stdin_selector(self):
        item = self.session()
        expected = ['/owned/vz', 'exec', '--environment', 'environment', '--machine', 'machine']
        public = item.exec_argv('source-fixed')
        private = item.exec_argv('source-fixed', stdin=True)
        self.assertEqual(public[:6], expected)
        self.assertEqual(private[:6], expected)
        self.assertIn('--no-stdin', public)
        self.assertNotIn('--no-stdin', private)
        self.assertEqual(private[6:], ['--timeout', '30', '--', '/bin/busybox', 'sh', '-c', 'source-fixed'])

    def test_public_exec_rejects_nonzero_and_stderr(self):
        item = self.session()
        for result in ((b'output', b'', 1), (b'output', b'error', 0)):
            self.harness.command.return_value = result
            with self.assertRaises(ValueError):
                item.public_exec('observation', 'source-fixed')

    def test_prepare_collision_is_sticky_before_any_mutation(self):
        item = self.session()
        self.harness.docker.return_value = (json.dumps({'Name': SPEC['network_name']}).encode() + b'\n', b'', 0)
        with self.assertRaises(ValueError):
            item.prepare()
        self.assertTrue(item.failed)
        self.assertFalse(item.prepared)
        self.harness.mutate.assert_not_called()
        self.owned_commands.private.assert_not_called()
        for canary in self.private.canaries():
            self.assertIn(canary, self.harness.record.canaries)
            self.assertIn(canary, self.harness.sensitive_canaries)
        with self.assertRaises(ValueError):
            item.cleanup()

    def test_prepare_real_guest_fixed_ack_seam(self):
        item = self.session()
        with patch.object(item, 'private_exec', side_effect=RuntimeError('stop at private setup')) as private:
            with self.assertRaisesRegex(RuntimeError, 'stop at private setup'):
                item.prepare()
        self.assertTrue(item.failed)
        private.assert_called_once()
        label, script, payload, acknowledgment = private.call_args.args
        self.assertEqual(label, 'setup')
        self.assertEqual(script, subject.guest.setup_script(item.plan))
        self.assertEqual(acknowledgment, subject.guest.fixed_ack(item.plan, action='SETUP'))
        self.assertTrue(payload.startswith(b'VZ_REGISTRY_PRIVATE_V1\n'))
        self.assertEqual(self.harness.mutate.call_count, 1)

    def test_document_canary_rejected_without_write(self):
        item = self.session()
        before = self.mocks[3].call_count
        with self.assertRaises(ValueError):
            item.document('public.json', {'secret': '1' * 48})
        self.assertEqual(self.mocks[3].call_count, before)

    def test_login_uses_exact_config_context_and_private_password(self):
        item = self.session(); item.prepared = True
        item.login(case='valid', role='valid', expected_stdout=b'Login Succeeded\n',
                   expected_stderr=b'', expected_exit=0)
        args, options = self.owned_commands.private.call_args
        self.assertEqual(args[0], 'login-valid')
        self.assertEqual(args[1], ['docker', '--config', '/owned/machine-config', '--context',
            'exact-context', 'login', '--username', 'vz-registry-user', '--password-stdin', SPEC['authority']])
        self.assertEqual(options['private_input'], b'1' * 48 + b'\n')
        self.assertNotIn('1' * 48, ' '.join(args[1]))
        self.assertEqual(options['executable'], '/owned/docker')
        self.store.check_transition.assert_called_once_with(item.initial_credentials, expected='login')


if __name__ == '__main__':
    unittest.main()
