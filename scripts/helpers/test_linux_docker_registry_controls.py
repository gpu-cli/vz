"""Real private temp trees and durable fake receipts; no Docker, vz or VM dispatch."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_registry_controls as subject
import linux_docker_registry_credentials as credentials
import linux_docker_registry_fixture as fixture

VALID = b'a192c582b772d45e13c7b6da90c81452a192c582b772d45e13c7b6da90c81452'
INVALID = b'0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f'
HTTP_SECRET = b'private-http-secret-77bd1e'
RUN = 'run'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class ControlsBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / 'home'
        (self.home / '.docker').mkdir(parents=True)
        (self.home / '.docker' / 'config.json').write_bytes(b'{"currentContext":"orbstack"}\n')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        environment = patch.dict(os.environ, {'HOME': str(self.home), 'PATH': str(self.bin)}, clear=False)
        environment.start()
        self.addCleanup(environment.stop)
        os.environ.pop('DOCKER_CONFIG', None)
        config = self.root / 'docker'
        plugins = config / 'cli-plugins'
        plugins.mkdir(parents=True)
        for name in ('docker-compose', 'docker-buildx'):
            (plugins / name).write_bytes(b'plugin ' + name.encode())
        (config / 'config.json').write_bytes(b'{"currentContext":"default"}\n')
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir(mode=0o700)
        self.cli = self.root / 'vz'
        self.cli.write_bytes(b'vz')
        self.docker = str(self.root / 'docker-bin')
        self.harness = SimpleNamespace(config=config, evidence=self.evidence, cli=self.cli,
            env={'PATH': str(self.bin), 'HOME': str(self.home), 'LC_ALL': 'C'},
            info={'run_id': RUN, 'clients': {'docker': {'canonical': self.docker}}},
            record=SimpleNamespace(receipts=[]), mutations=[])
        self.plugins = str(plugins)
        self.contexts = [self.machine(index) for index in range(4)]
        self.selected, self.sentinel = self.contexts[:3], self.contexts[3]
        self.authority = fixture.resource_spec(self.contexts[0]['owner'], RUN)['authority']

    def machine(self, index):
        owner = {'project_id': 'prj', 'environment_id': 'env_' + str(index // 2), 'machine_id': 'mch_' + str(index)}
        directory = self.root / ('machine-' + str(index)) / 'docker-client'
        directory.mkdir(parents=True, mode=0o700)
        info = directory.stat()
        self.write(directory / 'vz-owner.json', {'schema_version': 1, 'owner': owner, 'nonce': 'lop_' + ('%032x' % index),
                                                'directory': {'device': info.st_dev, 'inode': info.st_ino}})
        self.write(directory / 'config.json', self.empty_config())
        return {'owner': owner, 'config_dir': str(directory), 'name': 'context-' + str(index)}

    def empty_config(self):
        return {'auths': {}, 'credHelpers': dict(credentials.GUARD), 'currentContext': 'default',
                'cliPluginsExtraDirs': [self.plugins]}

    def login_config(self, password=VALID):
        result = self.empty_config()
        result['auths'] = {self.authority: {'auth': base64.b64encode(b'vz-registry-user:' + password).decode()}}
        return result

    def write(self, path, value, replace=False):
        target = Path(str(path) + '.new') if replace else Path(path)
        target.write_bytes(json.dumps(value).encode())
        target.chmod(0o600)
        if replace:
            target.replace(path)

    def controls(self):
        return subject.Controls(self.harness, self.contexts, self.selected, self.sentinel)


class PhaseTests(ControlsBase):
    def test_baseline_proves_distinctness_and_empty_state(self):
        proof = self.controls().baseline()
        self.assertEqual(proof['phase'], 'baseline')
        self.assertTrue(proof['all_four_empty'])
        self.assertTrue(proof['distinctness']['distinct_inodes'])
        self.assertEqual(proof['distinctness']['authority'], '172.30.241.2:5443')
        self.assertEqual([row['role'] for row in proof['distinctness']['machines']], ['selected'] * 3 + ['sentinel'])
        self.assertEqual(proof['host']['credential_helpers_on_path'], [])
        self.assertEqual(proof['host']['states']['home_config']['sha256'], sha(b'{"currentContext":"orbstack"}\n'))
        self.assertFalse(proof['private_config_published'])
        self.assertNotIn(VALID.decode(), json.dumps(proof))

    def test_shared_config_dir_fails_distinctness(self):
        self.contexts[3]['config_dir'] = self.contexts[2]['config_dir']
        with self.assertRaises(subject.ControlError):
            self.controls().baseline()

    def test_symlinked_config_dir_fails_distinctness(self):
        link = self.root / 'machine-link' / 'docker-client'
        link.parent.mkdir()
        os.symlink(self.contexts[2]['config_dir'], link)
        self.contexts[3]['config_dir'] = str(link)
        with self.assertRaises(subject.ControlError):
            self.controls().baseline()

    def test_loose_config_dir_mode_rejected(self):
        os.chmod(self.contexts[1]['config_dir'], 0o750)
        with self.assertRaises(subject.ControlError):
            self.controls().baseline()

    def test_topology_shape_rejected(self):
        with self.assertRaises(subject.ControlError):
            subject.Controls(self.harness, self.contexts[:3], self.selected, self.sentinel)
        with self.assertRaises(subject.ControlError):
            subject.Controls(self.harness, self.contexts, self.contexts[1:], self.contexts[1])
        with self.assertRaises(subject.ControlError):
            subject.Controls(self.harness, self.contexts, self.selected, {'owner': {'project_id': 'x',
                'environment_id': 'y', 'machine_id': 'z'}, 'config_dir': '/x/docker-client', 'name': 'foreign'})

    def test_baseline_rejects_preexisting_auth_entry(self):
        self.write(Path(self.contexts[3]['config_dir']) / 'config.json', self.login_config(), replace=True)
        with self.assertRaises(subject.ControlError):
            self.controls().baseline()

    def test_check_after_login_requires_baseline(self):
        with self.assertRaises(subject.ControlError):
            self.controls().check_after_login(self.contexts[0])

    def test_after_login_passes_with_only_active_changed(self):
        controls = self.controls()
        controls.baseline()
        self.write(Path(self.contexts[0]['config_dir']) / 'config.json', self.login_config(), replace=True)
        proof = controls.check_after_login(self.contexts[0])
        self.assertEqual(proof['active_auth_authorities'], [self.authority])
        self.assertEqual([row['role'] for row in proof['siblings']], ['sibling', 'sibling', 'sentinel'])
        self.assertTrue(all(row['unchanged_since_empty_baseline'] for row in proof['siblings']))
        self.assertTrue(proof['host_defaults_unchanged'])
        self.assertNotIn(base64.b64encode(b'vz-registry-user:' + VALID).decode(), json.dumps(proof))

    def test_after_login_requires_active_entry(self):
        controls = self.controls()
        controls.baseline()
        with self.assertRaises(subject.ControlError):
            controls.check_after_login(self.contexts[0])

    def test_after_login_rejects_sentinel_as_active(self):
        controls = self.controls()
        controls.baseline()
        self.write(Path(self.sentinel['config_dir']) / 'config.json', self.login_config(), replace=True)
        with self.assertRaises(subject.ControlError):
            controls.check_after_login(self.sentinel)

    def test_sibling_mutated_after_login_fails(self):
        for victim, mutation in ((1, self.login_config(INVALID)), (3, self.login_config()),
                                 (2, dict(self.empty_config(), currentContext='orbstack'))):
            with self.subTest(victim=victim):
                controls = self.controls()
                controls.baseline()
                self.write(Path(self.contexts[0]['config_dir']) / 'config.json', self.login_config(), replace=True)
                self.write(Path(self.contexts[victim]['config_dir']) / 'config.json', mutation, replace=True)
                with self.assertRaises(subject.ControlError):
                    controls.check_after_login(self.contexts[0])
                self.write(Path(self.contexts[victim]['config_dir']) / 'config.json', self.empty_config(), replace=True)
                self.write(Path(self.contexts[0]['config_dir']) / 'config.json', self.empty_config(), replace=True)

    def test_sibling_rewritten_with_identical_bytes_still_fails(self):
        controls = self.controls()
        controls.baseline()
        self.write(Path(self.contexts[0]['config_dir']) / 'config.json', self.login_config(), replace=True)
        self.write(Path(self.contexts[1]['config_dir']) / 'config.json', self.empty_config(), replace=True)
        with self.assertRaises(subject.ControlError):
            controls.check_after_login(self.contexts[0])

    def test_host_default_changed_fails(self):
        controls = self.controls()
        controls.baseline()
        self.write(Path(self.contexts[0]['config_dir']) / 'config.json', self.login_config(), replace=True)
        (self.home / '.docker' / 'config.json').write_bytes(b'{"currentContext":"orbstack","auths":{}}\n')
        with self.assertRaises(subject.ControlError):
            controls.check_after_login(self.contexts[0])

    def test_host_config_appearing_or_disappearing_fails(self):
        (self.home / '.docker' / 'config.json').unlink()
        controls = self.controls()
        proof = controls.baseline()
        self.assertEqual(proof['host']['states']['home_config'], {'present': False})
        (self.home / '.docker' / 'config.json').write_bytes(b'{}\n')
        with self.assertRaises(subject.ControlError):
            controls.final()

    def test_credential_helper_appearing_on_path_fails(self):
        controls = self.controls()
        controls.baseline()
        (self.bin / 'docker-credential-osxkeychain').write_bytes(b'helper')
        with self.assertRaises(subject.ControlError):
            controls.final()

    def test_shared_plugin_dir_change_fails(self):
        controls = self.controls()
        controls.baseline()
        (Path(self.plugins) / 'docker-compose').write_bytes(b'replaced plugin')
        with self.assertRaises(subject.ControlError):
            controls.final()

    def test_orbstack_context_change_fails(self):
        meta = self.home / '.docker' / 'contexts' / 'meta' / subject.ORBSTACK_CONTEXT
        meta.mkdir(parents=True)
        (meta / 'meta.json').write_bytes(b'{"Name":"orbstack"}')
        controls = self.controls()
        controls.baseline()
        (meta / 'meta.json').write_bytes(b'{"Name":"orbstack","Endpoints":{}}')
        with self.assertRaises(subject.ControlError):
            controls.final()

    def test_logout_refreshes_active_baseline_and_final_publishes_empty_hashes(self):
        controls = self.controls()
        controls.baseline()
        for index in range(3):
            active = self.contexts[index]
            self.write(Path(active['config_dir']) / 'config.json', self.login_config(), replace=True)
            controls.check_after_login(active)
            # Docker logout replaces the file: identical policy, new identity.
            self.write(Path(active['config_dir']) / 'config.json', self.empty_config(), replace=True)
            proof = controls.check_after_logout(active)
            self.assertTrue(proof['all_four_empty'])
            self.assertEqual([row['baseline_refreshed'] for row in proof['machines']],
                             [position == index for position in range(4)])
        final = controls.final()
        self.assertTrue(final['all_four_empty'] and final['host_defaults_unchanged'])
        self.assertEqual(len(final['machines']), 4)
        expected = sha(json.dumps(self.empty_config()).encode())
        self.assertTrue(all(row['empty_config_sha256'] == expected for row in final['machines']))
        self.assertEqual(len(controls.proofs), 1 + 3 * 2 + 1)

    def test_logout_with_lingering_entry_fails(self):
        controls = self.controls()
        controls.baseline()
        self.write(Path(self.contexts[0]['config_dir']) / 'config.json', self.login_config(), replace=True)
        controls.check_after_login(self.contexts[0])
        with self.assertRaises(subject.ControlError):
            controls.check_after_logout(self.contexts[0])

    def test_placeholder_stores_never_receive_a_real_password(self):
        observed = []
        original = credentials.Store.__init__

        def spy(store, descriptor, **kwargs):
            observed.append(kwargs['password'])
            return original(store, descriptor, **kwargs)
        with patch.object(credentials.Store, '__init__', spy):
            self.controls().baseline()
        self.assertEqual(len(observed), 4)
        self.assertEqual(len(set(observed)), 4)
        self.assertNotIn(VALID, observed)
        self.assertNotIn(INVALID, observed)


class ReplayBase(ControlsBase):
    def setUp(self):
        super().setUp()
        self.controls = subject.Controls(self.harness, self.contexts, self.selected, self.sentinel)
        self.controls.baseline()
        self.active = self.contexts[0]
        self.write(Path(self.active['config_dir']) / 'config.json', self.login_config(), replace=True)
        self.project = self.root / 'project'
        self.project.mkdir()
        self.session_output = self.evidence / 'registry-machine-0'
        self.session_output.mkdir(mode=0o700)
        (self.session_output / 'login-route.json').write_bytes(b'{"route": "inferred"}\n')
        (self.session_output / 'subject.tar').write_bytes(b'\0' * 1024)
        self.private_output = self.evidence / 'registry-private-0'
        self.private_output.mkdir(mode=0o700)
        self.output_dir = self.root / 'registry-controls'
        self.output_dir.mkdir()
        (Path(self.active['config_dir']) / 'contexts').mkdir()
        (Path(self.active['config_dir']) / 'contexts' / 'meta.json').write_bytes(b'{"Name":"context-0"}')
        self.private_receipts = []
        self.add_private('login-valid', ['docker', '--config', self.active['config_dir'], '--context', self.active['name'],
                                         'login', '--username', 'vz-registry-user', '--password-stdin', self.authority],
                         executable=self.docker, stdout=b'Login Succeeded\n', stderr=b'warning\n')
        self.add_private('setup', [str(self.cli), 'exec', '--environment', 'env_0', '--machine', 'mch_0', '--timeout', '30',
                                   '--', '/bin/busybox', 'sh', '-c', 'script'], executable=str(self.cli), stdout=b'ACK\n')
        self.host_receipts = []
        self.add_host('registry-server-logs', ['docker', '--config', self.active['config_dir'], '--context',
                                               self.active['name'], 'logs', 'c' * 64], executable=self.docker,
                      stdout=b'', stderr=b'{"msg":"listening on 172.30.241.2:5443, tls"}\n')
        self.add_host('registry-clock', [str(self.cli), 'exec', '--environment', 'env_0', '--machine', 'mch_0',
                                         '--no-stdin', '--', '/bin/busybox', 'date'], executable=str(self.cli),
                      stdout=b'1700000000\n', stderr=b'')
        self.add_host('registry-other', ['docker', '--config', self.contexts[1]['config_dir'], '--context',
                                         self.contexts[1]['name'], 'ps'], executable=self.docker, stdout=b'', stderr=b'')
        self.add_host('compose-up', ['docker', '--config', self.contexts[2]['config_dir'], '--context',
                                     self.contexts[2]['name'], 'compose', 'up'], executable=self.docker, stdout=b'x', stderr=b'')
        self.harness.mutations.append({'index': 1, 'label': 'registry-server-start', 'owner': dict(self.active['owner'])})
        self.write_json(self.evidence / 'mutation-001.intent.json', {'index': 1, 'effects_uncertain': True})
        self.write_json(self.evidence / 'mutation-001.result.json', {'index': 1, 'label': 'registry-server-start',
                        'effects_uncertain': False, 'error': None, 'exit_code': 0, 'command_index': 1})
        self.fixture = SimpleNamespace(canaries=lambda: (VALID, INVALID, HTTP_SECRET),
                                       password=lambda role='valid': VALID if role == 'valid' else INVALID)
        self.session = SimpleNamespace(credential_state='login', workload_complete=True, cleanup_complete=False,
            failed=False, descriptor=self.active, project=self.project, output=self.session_output,
            commands=SimpleNamespace(output=self.private_output, receipts=self.private_receipts),
            private_fixture=self.fixture, store=Mock())

    def write_json(self, path, value):
        Path(path).write_bytes(json.dumps(value, sort_keys=True).encode() + b'\n')

    def add_private(self, label, argv, *, executable, stdout, stderr=b'', exit=0):
        index = len(self.private_receipts) + 1
        capture = {'schema_version': 1, 'argv': argv, 'executable': executable, 'cwd': str(self.project),
                   'environment': dict(self.harness.env), 'expected_exit': exit, 'expected_stdout_sha256': sha(stdout),
                   'expected_stderr_sha256': sha(stderr), 'effects_uncertain': False, 'capture_complete': True,
                   'acknowledged': True, 'owned_process_reaped': True, 'returncode': exit, 'stdin_write_complete': True,
                   'stdin_eof_count': 1, 'error': None, 'unexpected_output_withheld': False,
                   'pending_process_retained': False, 'process_ownership_unresolved': False,
                   'private_input_hash_published': False, 'private_plan_published': False}
        row = {'index': index, 'label': label, 'capture': capture, 'durable_complete': True}
        self.private_receipts.append(row)
        stem = 'command-' + str(index).zfill(4) + '-' + label
        intent = dict(row, durable_complete=False, capture=dict(capture, effects_uncertain=True, acknowledged=False))
        self.write_json(self.private_output / (stem + '.intent.json'), intent)
        self.write_json(self.private_output / (stem + '.json'), row)
        (self.private_output / (stem + '.stdout')).write_bytes(stdout)
        (self.private_output / (stem + '.stderr')).write_bytes(stderr)
        return row, stem

    def add_host(self, label, argv, *, executable, stdout, stderr, exit=0):
        index = len(self.host_receipts) + 1
        row = {'index': index, 'label': label, 'argv': argv, 'argv0': argv[0], 'executable': executable,
               'cwd': str(self.project), 'effects_uncertain': False, 'capture_complete': True, 'exit_code': exit,
               'error': None, 'stdout_sha256': sha(stdout), 'stderr_sha256': sha(stderr),
               'retained_stdout_bytes': len(stdout), 'retained_stderr_bytes': len(stderr), 'hashes_cover': 'complete_streams'}
        self.host_receipts.append(row)
        self.harness.record.receipts.append(row)
        stem = str(index).zfill(3) + '-' + label
        self.write_json(self.evidence / (stem + '.intent.json'), {'index': index, 'label': label, 'effects_uncertain': True})
        self.write_json(self.evidence / (stem + '.result.json'), row)
        (self.evidence / (stem + '.stdout')).write_bytes(stdout)
        (self.evidence / (stem + '.stderr')).write_bytes(stderr)
        return row, stem

    def rewrite_private(self, position, **changes):
        row = self.private_receipts[position]
        row['capture'].update(changes)
        stem = 'command-' + str(row['index']).zfill(4) + '-' + row['label']
        (self.private_output / (stem + '.json')).write_bytes(json.dumps(row, sort_keys=True).encode() + b'\n')

    def replay(self):
        return self.controls.replay_and_scan(self.session, self.output_dir)


class ReplayTests(ReplayBase):
    def test_clean_replay_and_scan_passes(self):
        proof = self.replay()
        self.assertTrue(proof['independent_command_replay_complete'])
        self.assertEqual(proof['private_receipt_count'], 2)
        self.assertEqual([row['label'] for row in proof['host_receipts']], ['registry-server-logs', 'registry-clock'])
        self.assertTrue(proof['no_canary_found'] and proof['siblings_preserved'] and proof['host_defaults_preserved'])
        self.assertFalse(proof['release_certified'])
        scanned = {row['path'] for row in proof['scan']['files']}
        self.assertIn(str(self.session_output / 'subject.tar'), scanned)
        self.assertIn(str(self.evidence / '001-registry-server-logs.stderr'), scanned)
        self.assertIn(str(self.private_output / 'command-0001-login-valid.stdout'), scanned)
        self.assertIn(str(Path(self.active['config_dir']) / 'contexts' / 'meta.json'), scanned)
        self.assertNotIn(str(Path(self.active['config_dir']) / 'config.json'), scanned)
        self.assertNotIn(str(self.evidence / '003-registry-other.stdout'), scanned)
        self.assertEqual(proof['scan']['file_count'], len(scanned))
        self.assertTrue(all(len(row['sha256']) == 64 for row in proof['scan']['files']))
        self.session.store.snapshot.assert_called_once_with(expected='login')
        text = json.dumps(proof)
        for value in (VALID, INVALID, HTTP_SECRET, base64.b64encode(VALID), VALID.hex().encode()):
            self.assertNotIn(value.decode(), text)

    def test_refuses_without_login_state(self):
        for state in ('empty', None):
            self.session.credential_state = state
            with self.assertRaises(subject.ControlError):
                self.replay()
        self.session.credential_state = 'login'
        self.session.store.snapshot.side_effect = credentials.CredentialError('x')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_refuses_incomplete_or_cleaned_session(self):
        for field, value in (('workload_complete', False), ('cleanup_complete', True), ('failed', True)):
            with self.subTest(field=field):
                session = copy.copy(self.session)
                setattr(session, field, value)
                with self.assertRaises(subject.ControlError):
                    self.controls.replay_and_scan(session, self.output_dir)

    def test_sibling_mutation_before_cleanup_fails(self):
        self.write(Path(self.contexts[3]['config_dir']) / 'config.json', self.login_config(), replace=True)
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_uncertain_private_receipt(self):
        self.rewrite_private(0, effects_uncertain=True)
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_unreaped_or_unacknowledged_receipt(self):
        for change in ({'owned_process_reaped': False, 'process_ownership_unresolved': True},
                       {'acknowledged': False}, {'pending_process_retained': True}, {'error': 'transport_exception'},
                       {'returncode': 1}, {'private_input_hash_published': True}):
            with self.subTest(change=change):
                saved = copy.deepcopy(self.private_receipts[0])
                self.rewrite_private(0, **change)
                with self.assertRaises(subject.ControlError):
                    self.replay()
                self.private_receipts[0] = saved
                self.rewrite_private(0)

    def test_replay_rejects_live_durable_divergence(self):
        self.private_receipts[1]['capture']['returncode'] = 0
        self.private_receipts[1]['label'] = 'setup'
        self.private_receipts[1]['capture']['environment'] = dict(self.harness.env, EXTRA='1')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_stream_identity_mismatch(self):
        (self.private_output / 'command-0001-login-valid.stdout').write_bytes(b'Login Succeeded?\n')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_host_stream_mismatch(self):
        (self.evidence / '001-registry-server-logs.stderr').write_bytes(b'{"msg":"other"}\n')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_unpinned_executable(self):
        self.rewrite_private(0, executable='/usr/local/bin/docker')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_foreign_config_argument(self):
        argv = list(self.private_receipts[0]['capture']['argv'])
        argv[2] = self.contexts[1]['config_dir']
        self.rewrite_private(0, argv=argv)
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_uncertain_host_receipt(self):
        row = self.host_receipts[0]
        row['effects_uncertain'] = True
        self.write_json(self.evidence / '001-registry-server-logs.result.json', row)
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_registry_receipt_without_machine_binding(self):
        self.add_host('registry-stray', ['docker', '--config', '/elsewhere/docker-client', '--context', 'default', 'ps'],
                      executable=self.docker, stdout=b'', stderr=b'')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_uncertain_mutation(self):
        self.write_json(self.evidence / 'mutation-001.result.json', {'index': 1, 'label': 'registry-server-start',
                        'effects_uncertain': True, 'error': 'RuntimeError: x', 'exit_code': None})
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_replay_rejects_missing_or_extra_private_files(self):
        (self.private_output / 'command-0002-setup.stderr').unlink()
        with self.assertRaises(subject.ControlError):
            self.replay()
        (self.private_output / 'command-0002-setup.stderr').write_bytes(b'')
        (self.private_output / 'stray.bin').write_bytes(b'x')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_canary_scan_catches_raw_base64_hex_and_auth_blob_leaks(self):
        blob = b'vz-registry-user:' + VALID
        leaks = {'raw': VALID, 'invalid_raw': INVALID, 'http_secret': HTTP_SECRET,
                 'base64': base64.b64encode(VALID), 'urlsafe': base64.urlsafe_b64encode(INVALID),
                 'hex': VALID.hex().encode(), 'hex_upper': VALID.hex().upper().encode(),
                 'auth_blob_base64': base64.b64encode(blob), 'auth_blob_hex': blob.hex().encode(),
                 'json_row': json.dumps({'auth': base64.b64encode(blob).decode()}).encode()}
        for name, leak in leaks.items():
            for root in (self.session_output, self.output_dir, self.private_output):
                with self.subTest(name=name, root=root.name):
                    if root == self.private_output:
                        path = self.private_output / 'command-0002-setup.stdout'
                        original = path.read_bytes()
                        path.write_bytes(b'ACK\n' + leak)
                    else:
                        path = root / ('leak-' + name + '.log')
                        path.write_bytes(b'prefix\n' + leak + b'\nsuffix\n')
                    with self.assertRaises(subject.ControlError):
                        self.replay()
                    if root == self.private_output:
                        path.write_bytes(original)
                    else:
                        path.unlink()
        self.assertTrue(self.replay()['no_canary_found'])

    def test_canary_scan_covers_config_dir_cache_and_host_logs(self):
        (Path(self.active['config_dir']) / 'contexts' / 'meta.json').write_bytes(VALID)
        with self.assertRaises(subject.ControlError):
            self.replay()
        (Path(self.active['config_dir']) / 'contexts' / 'meta.json').write_bytes(b'{}')
        row = self.host_receipts[0]
        leaked = b'{"msg":"' + base64.b64encode(VALID) + b'"}\n'
        row.update(stderr_sha256=sha(leaked), retained_stderr_bytes=len(leaked))
        self.write_json(self.evidence / '001-registry-server-logs.result.json', row)
        (self.evidence / '001-registry-server-logs.stderr').write_bytes(leaked)
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_canary_scan_spans_window_boundaries(self):
        payload = b'\0' * (subject.WINDOW - 10) + VALID + b'\0' * 100
        (self.output_dir / 'large.bin').write_bytes(payload)
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_scan_rejects_symlinks_in_evidence_trees(self):
        os.symlink('/etc/hosts', self.output_dir / 'escape')
        with self.assertRaises(subject.ControlError):
            self.replay()

    def test_scan_bytes_clean_and_dirty(self):
        variants = subject.canary_variants(self.fixture)
        self.assertFalse(subject.scan_bytes(b'nothing here', variants))
        self.assertFalse(subject.scan_bytes(b'', variants))
        self.assertTrue(subject.scan_bytes(b'x' + VALID.hex().encode(), variants))
        self.assertIn(base64.b64encode(b'vz-registry-user:' + VALID), variants)
        self.assertIn(base64.b64encode(b'vz-registry-user:' + INVALID), variants)

    def test_canary_variants_require_passwords_among_canaries(self):
        fake = SimpleNamespace(canaries=lambda: (HTTP_SECRET,), password=lambda role='valid': VALID)
        with self.assertRaises(subject.ControlError):
            subject.canary_variants(fake)

    def test_username_matches_secret_fixture(self):
        import linux_docker_registry_secrets as secrets
        self.assertEqual(subject.USERNAME, secrets.USERNAME)


if __name__ == '__main__':
    unittest.main()
