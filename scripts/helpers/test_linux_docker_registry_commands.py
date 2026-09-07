"""Mock dispatch only; real bounded private evidence filesystem operations."""
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import linux_docker_registry_commands as subject

SECRET = b'private-unit-password-319875'


class FakeCapture:
    def __init__(self, argv, **kwargs):
        self.pending_process = None
        self.receipt = {'acknowledged': False, 'effects_uncertain': True, 'owned_process_reaped': False}

    def run(self):
        self.receipt.update(acknowledged=True, effects_uncertain=False, owned_process_reaped=True)
        return SimpleNamespace(stdout=b'ack\n', stderr=b'', returncode=0, receipt=dict(self.receipt), pending_process=None)


class CommandsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.descriptor = {'owner': {'project_id': 'prj_one', 'environment_id': 'env_one', 'machine_id': 'mch_one'},
                           'config_dir': str(self.root / 'docker-client'), 'name': 'machine-context'}
        for name in ('vz', 'docker'):
            path = self.root / name; path.write_bytes(b'public tool bytes ' + name.encode()); path.chmod(0o500)
        self.cli, self.docker = self.root / 'vz', str(self.root / 'docker')
        digest = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        self.h = SimpleNamespace(registry_commands=[], env={'LC_ALL': 'C'}, descriptors=[self.descriptor],
            cli=self.cli, staged_inputs={str(self.cli): digest(self.cli)}, evidence=self.root,
            info={'inputs': {self.docker: digest(self.docker)},
                  'clients': {'docker': {'canonical': self.docker, 'sha256': digest(self.docker)}}})
        self.owner = subject.Commands(self.h, self.descriptor, self.root, 0)
        self.argv = ['docker', '--config', self.descriptor['config_dir'], '--context', self.descriptor['name'],
                     'login', '--password-stdin', 'registry.invalid']

    def run_private(self):
        return self.owner.private('login', self.argv, executable=self.docker,
                                  private_input=SECRET, expected_stdout=b'ack\n')

    def test_registered_before_durable_intent_and_dispatch(self):
        self.assertIs(self.h.registry_commands[0], self.owner)
        test = self
        class Observed(FakeCapture):
            def run(inner):
                test.assertIs(test.owner.owners[0], inner)
                intent = test.owner.output / 'command-0001-login.intent.json'
                test.assertTrue(intent.is_file())
                test.assertTrue(json.loads(intent.read_bytes())['capture']['effects_uncertain'])
                return super().run()
        with patch.object(subject.private_stdin, 'Capture', Observed): self.run_private()
        self.owner.assert_certain()
        for path in self.owner.output.iterdir():
            self.assertNotIn(SECRET, path.read_bytes())
        with self.assertRaises(subject.CommandError): self.run_private()

    def test_intent_write_failure_retains_owner_without_dispatch(self):
        with patch.object(subject.private_stdin, 'Capture', FakeCapture), \
             patch.object(self.owner, '_persist', side_effect=OSError(SECRET.decode())), \
             patch.object(FakeCapture, 'run') as run:
            with self.assertRaises(subject.CommandError) as error: self.run_private()
        self.assertNotIn(SECRET.decode(), str(error.exception))
        self.assertEqual(len(self.owner.owners), 1); run.assert_not_called()
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()

    def test_terminal_write_failure_remains_fenced(self):
        original = self.owner._persist
        def fail(name, raw):
            if name.endswith('.stdout'): raise OSError('failure')
            return original(name, raw)
        with patch.object(subject.private_stdin, 'Capture', FakeCapture), patch.object(self.owner, '_persist', side_effect=fail):
            with self.assertRaises(subject.CommandError): self.run_private()
        self.assertTrue(self.owner.owners[0].receipt['owned_process_reaped'])
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()

    def test_pending_process_retained(self):
        pending = object()
        class Pending(FakeCapture):
            def run(inner):
                inner.pending_process = pending
                return SimpleNamespace(stdout=b'', stderr=b'', receipt=dict(inner.receipt), pending_process=pending)
        with patch.object(subject.private_stdin, 'Capture', Pending), self.assertRaises(subject.CommandError): self.run_private()
        self.assertIs(self.owner.owners[0].pending_process, pending)
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()

    def test_wrong_routing_and_executable_never_dispatch(self):
        self.argv[4] = 'default'
        with patch.object(subject.private_stdin, 'Capture') as capture, self.assertRaises(subject.CommandError): self.run_private()
        capture.assert_not_called()

    def test_pin_environment_descriptor_drift_is_sticky(self):
        self.h.env['EXTRA'] = 'drift'
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()
        del self.h.env['EXTRA']
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()

    def test_missing_pin_existing_output_and_changed_binary(self):
        with self.assertRaises(subject.CommandError): subject.Commands(self.h, self.descriptor, self.root, 0)
        Path(self.docker).chmod(0o700); Path(self.docker).write_bytes(b'changed')
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()

    def test_vz_exact_machine_prefix(self):
        argv = [str(self.cli), 'exec', '--environment', 'env_one', '--machine', 'mch_one', '--', '/bin/busybox', 'sh']
        with patch.object(subject.private_stdin, 'Capture', FakeCapture):
            self.owner.private('guest-stage', argv, executable=self.cli, private_input=SECRET, expected_stdout=b'ack\n')
        self.owner.assert_certain()

    def test_private_label_never_persisted(self):
        with self.assertRaises(subject.CommandError):
            self.owner.private('private-label', self.argv, executable=self.docker,
                private_input=b'private-label', expected_stdout=b'ack\n')
        self.assertEqual(list(self.owner.output.iterdir()), [])

    def test_changed_descriptor_and_registry_membership_rejected(self):
        self.descriptor['name'] = 'foreign'
        with self.assertRaises(subject.CommandError): self.owner.assert_certain()


if __name__ == '__main__':
    unittest.main()
