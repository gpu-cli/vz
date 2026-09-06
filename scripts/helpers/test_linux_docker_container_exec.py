"""Offline exec orchestration/replay adversaries; no Docker dispatch."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import linux_docker_container_exec as subject
import linux_docker_container_fixture as fixture

CID = 'a' * 64
TOKEN = 'vzio-' + 'b' * 24


class Exec(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='vz-exec-mock-')
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name).resolve()
        self.inputs = {'docker_config': '/private/config', 'scope': {'docker_context': 'owned-machine'},
                       'clients': {'docker': {'path': '/pinned/docker'}}}
        self.env = {'PATH': '/usr/bin:/bin'}
        self.events, self.raw, self.receipts = [], {}, {}
        self.mutate = lambda name, stdout, stderr: (stdout, stderr)
        self.item = SimpleNamespace(output=self.output, inputs=SimpleNamespace(raw=self.inputs), env=self.env)
        self.item.guard = lambda: self.events.append('machine-guard')
        self.item.command = self.command
        self.item.record = SimpleNamespace(max_stream_bytes=subject.LIMIT, acknowledge_negative=self.acknowledge)
        validator = patch.object(subject.interactive, 'validate_recorded', side_effect=self.validate)
        self.validator = validator.start()
        self.addCleanup(validator.stop)

    def service_guard(self, cid, token):
        self.assertEqual((cid, token), (CID, TOKEN))
        self.events.append('service-guard')

    def outputs(self, name):
        marker = lambda value: fixture.marker(TOKEN, value)
        if name in ('root', 'nonroot'):
            uid = 0 if name == 'root' else 10001
            namespaces = {name: name + ':[123]' for name in fixture.NAMESPACES}
            row = {'schema_version': 1, 'type': 'exec_identity', 'token': TOKEN, 'uid': uid, 'gid': uid,
                   'cwd': '/workspace', 'pid': 7 if uid == 0 else 8, 'pid1': 1,
                   'root_marker': 'vz-container-io-root-v1\n', 'namespaces': namespaces,
                   'pid1_namespaces': namespaces if uid == 0 else
                       {name: {'error': 'permission_denied', 'errno': 13} for name in fixture.NAMESPACES}}
            return fixture.encode(row) + b'\n' + marker('exec-stdout'), marker('exec-stderr')
        if name == 'stream':
            return (marker('stdout-begin') + fixture.INPUT + b'\n' + marker('stdout-end'),
                    marker('stderr-begin') + marker('stderr-end'))
        rows = [{'schema_version': 1, 'type': 'tty_ready', 'token': TOKEN,
                 'isatty': [True, True, True], 'rows': 24, 'cols': 80}]
        if name == 'tty-exit':
            rows.extend([{'schema_version': 1, 'type': 'tty_resized', 'token': TOKEN, 'rows': 40, 'cols': 120},
                         {'schema_version': 1, 'type': 'tty_size', 'token': TOKEN, 'rows': 40, 'cols': 120},
                         {'schema_version': 1, 'type': 'tty_done', 'token': TOKEN, 'exit_code': 37}])
        else:
            rows.append({'schema_version': 1, 'type': 'observed_signal', 'token': TOKEN,
                         'signal': 'SIGINT', 'exit_code': 130})
        return b''.join(fixture.encode(row) + b'\r\n' for row in rows), b''

    def command(self, args, *, expected, timeout, interaction_plan):
        index = len(self.raw) + 1
        operation = subject.operations(CID, TOKEN)[index-1]
        self.assertIsNone(expected)
        self.assertEqual(timeout, 30)
        self.assertEqual(args, operation['args'])
        self.assertEqual(interaction_plan, operation['plan'])
        self.events.append('command-' + operation['name'])
        stdout, stderr = self.mutate(operation['name'], *self.outputs(operation['name']))
        for name, data in (('stdout', stdout), ('stderr', stderr)):
            (self.output / ('command-%05d.%s' % (index, name))).write_bytes(data)
        self.raw[index] = (stdout, stderr)
        self.receipts[index] = {'command_index': index, 'terminal_receipt_sha256': str(index) * 64}
        return SimpleNamespace(index=index, stdout=stdout, stderr=stderr, returncode=operation['exit'])

    def validate(self, output, index, *, argv, executable, env, expected_exit, expected_plan):
        self.events.append('validate-%d' % index)
        operation = subject.operations(CID, TOKEN)[index-1]
        if (Path(output) != self.output or argv != subject.invocation(self.inputs, operation['args']) or
                executable != '/pinned/docker' or env != self.env or expected_exit != operation['exit'] or
                expected_plan != operation['plan']):
            raise ValueError('mock independent invocation mismatch')
        return copy.deepcopy(self.receipts[index])

    def acknowledge(self, command, assertion):
        self.events.append('ack-%d' % command.index)
        row = {'command_index': command.index, 'assertion': assertion,
               'terminal_receipt_sha256': self.receipts[command.index]['terminal_receipt_sha256'],
               'effects_uncertain': False}
        (self.output / ('command-%05d.acknowledgement.json' % command.index)).write_text(json.dumps(row))

    def run_all(self):
        return subject.run_exec_io(self.item, CID, TOKEN, service_guard=self.service_guard)

    def replay(self, observations, **overrides):
        return subject.replay_exec_io(self.output, overrides.get('inputs', self.inputs),
            overrides.get('cid', CID), TOKEN, observations, environment=overrides.get('environment', self.env))

    def test_all_five_success_guard_order_and_complete_replay(self):
        observation = self.run_all()
        self.assertEqual(self.replay(observation), observation)
        self.assertEqual(len(observation['operations']), 5)
        for index, operation in enumerate(subject.operations(CID, TOKEN), 1):
            first = self.events.index('command-' + operation['name'])
            self.assertEqual(self.events[first-2:first+5], ['machine-guard', 'service-guard',
                'command-' + operation['name'], 'validate-%d' % index,
                'machine-guard', 'service-guard', 'ack-%d' % index])
        self.assertIn('service_lifecycle_guard_replay_required', observation['scope'])

    def test_fixed_binary_eof_and_terminal_input_not_cli_signal(self):
        root, nonroot, stream, tty, interrupt = subject.operations(CID, TOKEN)
        self.assertIn('10001:10001', nonroot['args'])
        self.assertEqual(len(stream['plan']['actions'][0]['data']), 65792)
        self.assertEqual(stream['plan']['actions'][1], {'kind': 'close_stdin'})
        self.assertIn('--interactive', stream['args'])
        self.assertIn('--tty', tty['args'])
        self.assertEqual(tty['plan']['actions'][0]['rows'], 40)
        self.assertEqual(tty['plan']['actions'][0]['cols'], 120)
        self.assertIs(tty['plan']['defer_sigwinch'], True)
        for operation in (root, nonroot, stream, interrupt):
            self.assertNotIn('defer_sigwinch', operation['plan'])
        resized = fixture.encode({'schema_version': 1, 'type': 'tty_resized', 'token': TOKEN,
                                  'rows': 40, 'cols': 120}) + b'\r\n'
        self.assertEqual(tty['plan']['actions'][1], {'kind': 'write', 'data': b'size\n',
            'after': {'stream': 'tty', 'marker': resized}})
        self.assertEqual(interrupt['plan']['actions'][0]['data'], b'\x03')
        self.assertFalse(any(a['kind'] == 'signal' for a in interrupt['plan']['actions']))
        for operation in (root, nonroot, stream, tty, interrupt):
            self.assertEqual(subject.interactive.decode_plan(subject.interactive.encode_plan(operation['plan'])),
                             operation['plan'])

    def test_missing_service_guard_and_foreign_identity_reject_before_dispatch(self):
        for cid, token, guard in ((CID, TOKEN, None), ('short-id', TOKEN, self.service_guard),
                                   (CID, 'foreign-token', self.service_guard)):
            with self.subTest(cid=cid, token=token), self.assertRaises(ValueError):
                subject.run_exec_io(self.item, cid, token, service_guard=guard)
            self.assertEqual(self.raw, {})

    def test_capture_failure_withholds_ack_and_later_dispatch(self):
        self.validator.side_effect = ValueError('capture incomplete')
        with self.assertRaises(ValueError): self.run_all()
        self.assertEqual(len(self.raw), 1)
        self.assertFalse(any(event.startswith('ack-') for event in self.events))

    def test_semantic_binary_tty_user_and_namespace_failures_withhold_ack(self):
        for phase, before, after in (('stream', b'stdout-begin', b'foreign-begin'),
                ('tty-exit', b'"cols":120', b'"cols":121'),
                ('tty-sigint', b'SIGINT', b'SIGTERM'),
                ('nonroot', b'"uid":10001', b'"uid":0'),
                ('nonroot', b'pid:[123]', b'pid:[124]')):
            with self.subTest(phase=phase, before=before):
                # Each failure uses fresh, unrelated temporary evidence.
                other = Exec('test_fixed_binary_eof_and_terminal_input_not_cli_signal')
                other.setUp()
                try:
                    other.mutate = lambda name, out, err: (out.replace(before, after), err) if name == phase else (out, err)
                    with self.assertRaises(ValueError): other.run_all()
                    index = [p['name'] for p in subject.operations(CID, TOKEN)].index(phase) + 1
                    self.assertEqual(len(other.raw), index)
                    self.assertNotIn('ack-%d' % index, other.events)
                finally: other.doCleanups()

    def test_post_service_guard_failure_withholds_ack(self):
        calls = []
        def guard(cid, token):
            calls.append(cid)
            if len(calls) == 2: raise ValueError('service restarted')
        with self.assertRaises(ValueError):
            subject.run_exec_io(self.item, CID, TOKEN, service_guard=guard)
        self.assertEqual(len(self.raw), 1)
        self.assertNotIn('ack-1', self.events)

    def test_observation_mutations_rejected(self):
        observation = self.run_all()
        for key in ('missing', 'reorder', 'duplicate', 'bool-index', 'foreign-container', 'foreign-proof', 'extra'):
            row = copy.deepcopy(observation)
            if key == 'missing': row['operations'].pop()
            elif key == 'reorder': row['operations'].reverse()
            elif key == 'duplicate': row['operations'][1] = row['operations'][0]
            elif key == 'bool-index': row['operations'][0]['command_index'] = True
            elif key == 'foreign-container': row['container_id'] = 'c' * 64
            elif key == 'foreign-proof': row['operations'][0]['semantic']['uid'] = False
            else: row['extra'] = True
            with self.subTest(key=key), self.assertRaises(ValueError): self.replay(row)

    def test_acknowledgement_digest_or_assertion_changes_rejected(self):
        observation = self.run_all()
        path = self.output / 'command-00001.acknowledgement.json'
        original = json.loads(path.read_bytes())
        for key in ('terminal_receipt_sha256', 'assertion', 'effects_uncertain'):
            row = dict(original); row[key] = True
            path.write_text(json.dumps(row))
            with self.subTest(key=key), self.assertRaises(ValueError): self.replay(observation)

    def test_foreign_external_environment_or_context_rejected(self):
        observation = self.run_all()
        with self.assertRaises(ValueError): self.replay(observation, environment={'PATH': '/foreign'})
        foreign = copy.deepcopy(self.inputs); foreign['scope']['docker_context'] = 'foreign'
        with self.assertRaises(ValueError): self.replay(observation, inputs=foreign)


if __name__ == '__main__':
    unittest.main()
