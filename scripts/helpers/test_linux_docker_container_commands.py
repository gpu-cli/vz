"""Real Recorder persistence with fake dispatch; never launches any process."""
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import docker_host_driver as driver
import linux_docker_container_commands as replay


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='vz-container-command-replay-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.inputs = {'docker_config': '/owned/config', 'clients': {'docker': {'path': '/owned/docker', 'sha256': 'a'*64}},
                       'scope': {'docker_context': 'owned-context', 'docker_endpoint': 'unix:///owned/docker.sock',
                                 'engine_id': 'owned-engine', 'project_id': 'project', 'environment_id': 'environment',
                                 'machine_id': 'machine', 'machine_incarnation': 'incarnation'}}
        self.recorder = driver.Recorder(self.root, {'PATH': '/usr/bin:/bin'}, [])

    def record(self, args, code=0, stdout=b'public\n', stderr=b'', env=None):
        argv = ['docker', '--config', '/owned/config', '--context', 'owned-context', *args]
        result = SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr)
        with patch.object(driver, 'execute', return_value=result) as execute:
            command = self.recorder.run(argv, executable='/owned/docker', extra_env=env,
                                       mutation=replay.mutation_for(args))
            self.assertEqual(execute.call_count, 1)
        return command

    def validate(self, args, index=1, **kwargs):
        return replay.validate_command(self.root, index, self.inputs, args=args, **kwargs)

    def rewrite(self, name, callback):
        path = self.root / name
        value = json.loads(path.read_bytes()); callback(value)
        path.write_text(json.dumps(value))

    def test_recorder_source_compatible_positive_and_no_dispatch_replay(self):
        args = ['container', 'inspect', 'a'*64]
        self.record(args, stdout=b'\x00public\xff\n')
        with patch.object(driver, 'execute', side_effect=AssertionError('replay dispatched')):
            result = self.validate(args)
        self.assertEqual(result['stdout'], b'\x00public\xff\n')
        self.assertFalse(result['receipt']['mutation'])
        self.assertIsNone(result['acknowledgement'])

    def test_mutation_classification_is_source_exact(self):
        for args in (['info'], ['version'], ['logs', 'owned'], ['events'], ['context', 'inspect', 'x'],
                     ['image', 'inspect', 'x'], ['container', 'ls'], ['network', 'ls'], ['volume', 'inspect', 'x'],
                     ['compose', 'version'], ['buildx', 'inspect', 'x']):
            self.assertFalse(replay.mutation_for(args))
        for args in (['exec', 'owned', 'true'], ['wait', 'owned'], ['inspect', 'owned'],
                     ['container', 'create'], ['image', 'ls'], ['compose', 'ps'], ['start', 'owned']):
            self.assertTrue(replay.mutation_for(args))
        self.record(['info'])
        with self.assertRaises(ValueError):
            self.validate(['info'], mutation=True)

    def test_negative_requires_hashbound_ack_and_optional_exact_semantics(self):
        args = ['exec', 'owned', 'python3', '/fixture/probe.py', 'exec', 'token']
        command = self.record(args, 37, stderr=b'public-stderr\n')
        with self.assertRaises(ValueError):
            self.validate(args, expected_exit=37)
        result = self.validate(args, expected_exit=37, require_ack=False)
        self.assertTrue(result['receipt']['effects_uncertain'])
        self.recorder.acknowledge_negative(command, 'exact owned exec transcript and 37')
        self.validate(args, expected_exit=37, expected_acknowledgement='exact owned exec transcript and 37')
        with self.assertRaises(ValueError):
            self.validate(args, expected_exit=37, expected_acknowledgement='unrelated assertion')
        self.rewrite('command-00001.acknowledgement.json', lambda r: r.update(terminal_receipt_sha256='f'*64))
        with self.assertRaises(ValueError):
            self.validate(args, expected_exit=37)

    def test_route_environment_intent_and_type_drift(self):
        args = ['info']; self.record(args, env={'PUBLIC': 'value'})
        original = (self.root/'command-00001.json').read_bytes()
        for key, value in [('argv0', '/owned/docker'), ('executable', '/other/docker'), ('index', True),
                           ('environment', {'PUBLIC': 'other'}), ('started_unix_ns', True),
                           ('elapsed_ns', 131*10**9), ('mutation', 0), ('max_stream_bytes', True),
                           ('host_outcome', 'unknown'), ('capture_complete', 1), ('raw_streams_retained', False),
                           ('timed_out', True), ('interrupted', True), ('dispatch_error', 'failure'),
                           ('secret_leak_detected', True), ('exit_code', False), ('effects_uncertain', True)]:
            with self.subTest(key=key):
                (self.root/'command-00001.json').write_bytes(original)
                self.rewrite('command-00001.json', lambda r: r.update({key: value}))
                with self.assertRaises(ValueError):
                    self.validate(args, extra_env={'PUBLIC': 'value'})
        (self.root/'command-00001.json').write_bytes(original)
        with self.assertRaises(ValueError):
            self.validate(['info', '--format', '{{json .}}'], extra_env={'PUBLIC': 'value'})
        with self.assertRaises(ValueError):
            self.validate(args)
        self.rewrite('command-00001.intent.json', lambda r: r.update(effects_uncertain=False))
        with self.assertRaises(ValueError):
            self.validate(args, extra_env={'PUBLIC': 'value'})

    def test_raw_hash_counts_paths_and_interaction_rejected(self):
        args = ['info']; self.record(args)
        original = (self.root/'command-00001.json').read_bytes()
        changes = [('stdout', '../foreign'), ('raw_stdout_sha256', 'f'*64),
                   ('retained_observed_stdout_bytes', True), ('observed_bytes', {'stdout': True, 'stderr': 0}),
                   ('interaction_capture', {}), ('retry_count', 1)]
        for key, value in changes:
            (self.root/'command-00001.json').write_bytes(original)
            self.rewrite('command-00001.json', lambda r: r.update({key: value}))
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate(args)
        (self.root/'command-00001.json').write_bytes(original)
        (self.root/'command-00001.interaction-plan.json').write_text('{}')
        with self.assertRaises(ValueError):
            self.validate(args)

    def test_nonregular_and_duplicate_evidence_rejected_without_blocking(self):
        args = ['info']; self.record(args)
        target = self.root/'command-00001.stdout'
        target.unlink(); os.mkfifo(target)
        with self.assertRaises(ValueError):
            self.validate(args)
        target.unlink(); (self.root/'source').write_bytes(b'public\n'); target.symlink_to(self.root/'source')
        with self.assertRaises(OSError):
            self.validate(args)
        target.unlink(); os.link(self.root/'source', target)
        with self.assertRaises(ValueError):
            self.validate(args)
        target.unlink(); target.write_bytes(b'public\n')
        path = self.root/'command-00001.json'
        path.write_bytes(path.read_bytes().replace(b'"index": 1', b'"index": 1, "index": 1'))
        with self.assertRaises(ValueError):
            self.validate(args)

    def test_stale_ack_unknown_fields_and_erased_uncertainty_rejected(self):
        args = ['start', 'owned']; command = self.record(args, 1)
        self.recorder.acknowledge_negative(command, 'semantic denial')
        self.rewrite('command-00001.json', lambda r: r.update(effects_uncertain=False))
        with self.assertRaises(ValueError):
            self.validate(args, expected_exit=1)
        with self.assertRaises(ValueError):
            replay.decode(b'{"x":NaN}')

    def guard_rows(self):
        contexts = [{'Name': 'owned-context', 'Endpoints': {'docker': {'Host': 'unix:///owned/docker.sock', 'SkipTLSVerify': False}}}]
        info = {'ID': 'owned-engine', 'OSType': 'linux', 'Architecture': 'arm64', 'DefaultRuntime': 'youki',
                'Runtimes': {'youki': {'path': '/mnt/linux-bin/youki'}}}
        return contexts, info

    def record_guard(self, contexts=None, info=None):
        defaults = self.guard_rows()
        self.record(['context', 'inspect', 'owned-context'], stdout=json.dumps(defaults[0] if contexts is None else contexts).encode())
        self.record(['info', '--format', '{{json .}}'], stdout=json.dumps(defaults[1] if info is None else info).encode())

    def test_guard_without_current_socket_lookup(self):
        self.record_guard()
        proof = replay.validate_guard(self.root, self.inputs, 1, 2)
        self.assertEqual(proof['info']['ID'], 'owned-engine')
        self.assertTrue(proof['public_up_authority_required'])
        with self.assertRaises(ValueError):
            replay.validate_guard(self.root, self.inputs, 1, 3)

    def test_guard_foreign_context_and_engine(self):
        for kind in ('context', 'endpoint', 'tls', 'engine', 'target', 'runtime', 'path', 'alternate', 'inert'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory(dir=self.root) as temp:
                old_root, old_recorder = self.root, self.recorder
                self.root = Path(temp); self.recorder = driver.Recorder(self.root, {}, [])
                try:
                    contexts, info = self.guard_rows()
                    if kind == 'context': contexts[0]['Name'] = 'other'
                    if kind == 'endpoint': contexts[0]['Endpoints']['docker']['Host'] = 'unix:///other.sock'
                    if kind == 'tls': contexts[0]['Endpoints']['docker']['SkipTLSVerify'] = 0
                    if kind == 'engine': info['ID'] = 'other'
                    if kind == 'target': info['OSType'] = 'windows'
                    if kind == 'runtime': info['DefaultRuntime'] = 'runc'
                    if kind == 'path': info['Runtimes']['youki']['path'] = '/bin/runc'
                    if kind == 'alternate': info['Runtimes']['crun'] = {'path': 'crun'}
                    if kind == 'inert': info['Runtimes']['runc'] = {'path': 'runc'}
                    self.record_guard(contexts, info)
                    with self.assertRaises(ValueError):
                        replay.validate_guard(self.root, self.inputs, 1, 2)
                finally:
                    self.root, self.recorder = old_root, old_recorder

    def test_guard_calls_existing_runtime_proof_and_rejects_bad_binding(self):
        self.record_guard()
        self.inputs['runtime_evidence'] = {'bad': 'shape'}
        with self.assertRaises(ValueError):
            replay.validate_guard(self.root, self.inputs, 1, 2)

    def test_long_deadline_requires_explicit_source_expectation(self):
        args = ['build', '/owned/fixture']; self.record(args)
        self.rewrite('command-00001.json', lambda r: r.update(elapsed_ns=300*10**9))
        with self.assertRaises(ValueError):
            self.validate(args)
        self.validate(args, expected_timeout_seconds=300)
        for value in (True, 0, 301, 300.0, '300'):
            with self.subTest(timeout=value), self.assertRaises(ValueError):
                self.validate(args, expected_timeout_seconds=value)
        self.rewrite('command-00001.json', lambda r: r.update(elapsed_ns=310*10**9+1))
        with self.assertRaises(ValueError):
            self.validate(args, expected_timeout_seconds=300)
        self.rewrite('command-00001.json', lambda r: r.update(elapsed_ns=40*10**9+1))
        with self.assertRaises(ValueError):
            self.validate(args, expected_timeout_seconds=30)


if __name__ == '__main__':
    unittest.main()
