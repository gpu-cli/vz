"""Finite host Python checks; no Docker, VM, credentials or release claims."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_interactive_capture as capture
import linux_docker_interactive_evidence as evidence


def plan(data=b'public\x00\xff\n', timeout=3, limit=driver.MAX_STREAM_BYTES):
    return {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': timeout,
            'input_limit': driver.MAX_STREAM_BYTES, 'output_limit': limit,
            'actions': [{'kind': 'write', 'data': data}, {'kind': 'close_stdin'}]}


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='vz-interactive-recorder-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.recorder = driver.Recorder(self.root, {'PATH': '/usr/bin:/bin'}, [])

    def run_child(self, script, selected, **kwargs):
        return self.recorder.run([sys.executable, '-c', script], executable=sys.executable,
            timeout=selected['timeout_seconds'], interaction_plan=selected, **kwargs)

    def test_plan_durable_before_dispatch_and_replay_bound_negative_exit(self):
        selected = plan()
        original = capture.capture
        def observed(*args, **kwargs):
            intent = json.loads((self.root / 'command-00001.intent.json').read_bytes())
            raw = (self.root / intent['interaction_plan']).read_bytes()
            self.assertEqual(intent['interaction_plan_sha256'], driver.sha256(raw))
            self.assertEqual(evidence.decode_plan(raw), selected)
            self.assertTrue(intent['effects_uncertain'])
            self.assertTrue(self.recorder.receipts[0]['effects_uncertain'])
            return original(*args, **kwargs)
        with patch.object(capture, 'capture', side_effect=observed):
            result = self.run_child('import sys;sys.stdout.buffer.write(sys.stdin.buffer.read());'
                                    'sys.stderr.buffer.write(b"done");sys.exit(37)', selected)
        terminal = json.loads((self.root / 'command-00001.json').read_bytes())
        raw = (self.root / terminal['interaction_plan']).read_bytes()
        evidence.validate_capture(raw, terminal['interaction_capture'], result.stdout, result.stderr,
            argv=result.argv, executable=sys.executable, cwd=self.root, env=self.recorder.env, expected_exit=37)
        proof = evidence.validate_recorded(self.root, result.index, argv=result.argv,
            executable=sys.executable, env=self.recorder.env, expected_exit=37, expected_plan=selected)
        self.assertEqual(proof['plan_sha256'], terminal['interaction_plan_sha256'])
        self.assertEqual(result.stdout, selected['actions'][0]['data'])
        self.assertEqual(result.stderr, b'done')
        self.assertTrue(terminal['raw_streams_retained'])
        self.assertTrue(terminal['effects_uncertain'])
        self.recorder.acknowledge_negative(result, 'unit: independently replayed exact echo, stderr and exit37')
        self.assertFalse(self.recorder.receipts[0]['effects_uncertain'])
        self.assertEqual(self.recorder.pending_interactions, [])

    def test_invalid_or_mismatched_plan_never_dispatches_or_creates_intent(self):
        for selected in (plan(timeout=4), plan(limit=1024), plan() | {'mode': 'unknown'}):
            with self.subTest(plan=selected), patch.object(capture, 'capture') as dispatch:
                with self.assertRaises(ValueError):
                    self.recorder.run([sys.executable], executable=sys.executable, timeout=3,
                                      interaction_plan=selected)
                dispatch.assert_not_called()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_private_input_and_marker_rejected_before_persistence(self):
        self.recorder.canaries = [b'never-publish-this-private-input']
        cases = [plan(data=self.recorder.canaries[0]), plan()]
        cases[1]['actions'][0]['after'] = {'stream': 'stdout', 'marker': self.recorder.canaries[0]}
        split = [self.recorder.canaries[0][:10], self.recorder.canaries[0][10:]]
        cases.append(plan() | {'actions': [{'kind': 'write', 'data': split[0],
            'after': {'stream': 'stdout', 'marker': b'public'}}, {'kind': 'write', 'data': split[1]}]})
        cases.append(plan() | {'actions': [{'kind': 'write', 'data': b'public',
            'after': {'stream': 'stdout', 'marker': part}} for part in split]})
        for selected in cases:
            with patch.object(capture, 'capture') as dispatch, self.assertRaises(driver.Rejected):
                self.run_child('pass', selected)
            dispatch.assert_not_called()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_incomplete_capture_retains_prefix_and_cannot_acknowledge(self):
        self.recorder = driver.Recorder(self.root, {'PATH': '/usr/bin:/bin'}, [], max_stream_bytes=1024)
        with self.assertRaisesRegex(driver.Rejected, 'incomplete'):
            self.run_child('import os,time;os.write(1,b"a"*8192);time.sleep(2)', plan(limit=1024))
        terminal = json.loads((self.root / 'command-00001.json').read_bytes())
        self.assertEqual((self.root / terminal['stdout']).read_bytes(), b'a' * 1024)
        self.assertTrue(terminal['effects_uncertain'])
        self.assertFalse(terminal['capture_complete'])
        self.assertFalse(terminal['raw_streams_retained'])
        self.assertTrue(terminal['interaction_capture']['owned_process_reaped'])
        with self.assertRaises(driver.Rejected):
            self.recorder.acknowledge_negative(driver.Command(1, [], 37, b'', b''), 'not allowed')

    def test_plan_persistence_failure_keeps_in_memory_uncertainty(self):
        with patch.object(self.recorder, 'persist', side_effect=OSError('injected fsync failure')), \
                patch.object(capture, 'capture') as dispatch, self.assertRaises(OSError):
            self.run_child('pass', plan())
        dispatch.assert_not_called()
        self.assertTrue(self.recorder.receipts[0]['effects_uncertain'])

    def test_unproven_reap_retains_direct_child_for_caller_disposition(self):
        selected = plan(timeout=.04)
        selected['actions'] = []
        with patch.object(capture, 'signal_owned', side_effect=PermissionError('injected denial')), \
                self.assertRaises(driver.Rejected):
            self.run_child('import time;time.sleep(.2)', selected)
        self.assertEqual(len(self.recorder.pending_interactions), 1)
        process = self.recorder.pending_interactions[0]
        process.wait(timeout=2)
        self.assertEqual(process.returncode, 0)
        self.assertTrue(self.recorder.receipts[0]['effects_uncertain'])
        self.assertFalse(self.recorder.receipts[0]['interaction_capture']['owned_process_reaped'])

    def test_driver_keeps_explicit_machine_context_for_interaction(self):
        item = driver.Driver.__new__(driver.Driver)
        executable = str(Path(sys.executable).resolve())
        item.inputs = SimpleNamespace(raw={'clients': {'docker': {
            'path': executable, 'sha256': driver.sha256(driver.regular(Path(executable)))}},
            'docker_config': str(self.root)}, scope={'docker_context': 'vz-owned-machine'})
        item.config_snapshot = 'unchanged'
        item.validate_config = Mock(return_value='unchanged')
        item.record = Mock()
        item.record.run.return_value = driver.Command(1, [], 37, b'', b'')
        selected = plan()
        item.command(['exec', '-i', 'exact-owned-cid', '/fixture/probe.py'], expected=37,
                     timeout=3, interaction_plan=selected)
        item.record.run.assert_called_once_with(['docker', '--config', str(self.root), '--context',
            'vz-owned-machine', 'exec', '-i', 'exact-owned-cid', '/fixture/probe.py'],
            executable=executable, timeout=3, extra_env=None, mutation=True, interaction_plan=selected)

    def test_plan_receipt_and_ordinary_commands_share_one_sequence(self):
        first = self.recorder.run([sys.executable, '-c', 'print("ordinary")'], executable=sys.executable)
        second = self.run_child('import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())', plan())
        self.assertEqual((first.index, second.index), (1, 2))
        self.assertNotIn('interaction_capture', self.recorder.receipts[0])
        self.assertEqual(self.recorder.receipts[1]['interaction_plan'], 'command-00002.interaction-plan.json')

    def test_independent_replay_rejects_changed_plan_raw_or_dispatch(self):
        result = self.run_child('import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())', plan())
        def replay():
            return evidence.validate_recorded(self.root, result.index, argv=result.argv,
                executable=sys.executable, env=self.recorder.env, expected_exit=0, expected_plan=plan())
        replay()
        for name in ('stdout', 'interaction-plan.json', 'intent.json'):
            target = self.root / ('command-00001.' + name)
            original = target.read_bytes()
            target.write_bytes(original + b' ')
            try:
                if name == 'intent.json':
                    row = json.loads(original); row['argv'] = ['foreign']
                    target.write_text(json.dumps(row))
                with self.assertRaises(ValueError):
                    replay()
            finally:
                target.write_bytes(original)
        replay()

    def test_resealed_changed_plan_cannot_replace_source_selected_plan(self):
        selected = plan()
        result = self.run_child('import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())', selected)
        path = self.root / 'command-00001.interaction-plan.json'
        changed = json.loads(path.read_bytes()); changed['timeout_seconds'] = 4
        path.write_text(json.dumps(changed))
        for suffix in ('intent.json', 'json'):
            target = self.root / ('command-00001.' + suffix)
            row = json.loads(target.read_bytes())
            row['interaction_plan_sha256'] = driver.sha256(path.read_bytes())
            if suffix == 'json':
                row['interaction_capture']['timeout_seconds'] = 4
            target.write_text(json.dumps(row))
        with self.assertRaisesRegex(ValueError, 'independently selected plan'):
            evidence.validate_recorded(self.root, result.index, argv=result.argv,
                executable=sys.executable, env=self.recorder.env, expected_exit=0, expected_plan=selected)

    def test_resealed_coherent_capture_clock_shift_rejected_by_recorder_anchor(self):
        selected = plan()
        result = self.run_child('import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())', selected)
        path = self.root / 'command-00001.json'
        original = path.read_bytes()
        for clock in ('unix_ns', 'monotonic_ns'):
            terminal = json.loads(original); captured = terminal['interaction_capture']
            for stamp in (captured['started'], captured['completed'],
                          *(value for action in captured['actions'] for value in
                            (action['triggered'], action['completed']))):
                stamp[clock] += 10 * evidence.CLOCK_TOLERANCE_NS
            # All intra-capture intervals/order remain coherent and all output
            # hashes match. Only the external recorder anchor detects reuse.
            path.write_text(json.dumps(terminal))
            with self.subTest(clock=clock), self.assertRaisesRegex(ValueError, 'recorder'):
                evidence.validate_recorded(self.root, result.index, argv=result.argv,
                    executable=sys.executable, env=self.recorder.env, expected_exit=0, expected_plan=selected)
        path.write_bytes(original)

    def test_recorder_monotonic_anchor_missing_boolean_changed_or_outside_capture_rejected(self):
        selected = plan()
        result = self.run_child('import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())', selected)
        paths = [self.root / 'command-00001.intent.json', self.root / 'command-00001.json']
        originals = [path.read_bytes() for path in paths]
        for mutation in ('missing', 'boolean', 'mismatch', 'after-start', 'too-early'):
            intent, terminal = [json.loads(raw) for raw in originals]
            if mutation == 'missing':
                del intent['started_monotonic_ns']; del terminal['started_monotonic_ns']
            elif mutation == 'boolean':
                intent['started_monotonic_ns'] = terminal['started_monotonic_ns'] = True
            elif mutation == 'mismatch':
                terminal['started_monotonic_ns'] += 1
            elif mutation == 'after-start':
                intent['started_monotonic_ns'] = terminal['started_monotonic_ns'] = terminal['interaction_capture']['started']['monotonic_ns'] + 1
            else:
                intent['started_monotonic_ns'] -= 10 * evidence.CLOCK_TOLERANCE_NS
                terminal['started_monotonic_ns'] = intent['started_monotonic_ns']
            for path, row in zip(paths, (intent, terminal)): path.write_text(json.dumps(row))
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, 'recorder'):
                evidence.validate_recorded(self.root, result.index, argv=result.argv,
                    executable=sys.executable, env=self.recorder.env, expected_exit=0, expected_plan=selected)
        for path, raw in zip(paths, originals): path.write_bytes(raw)


if __name__ == '__main__':
    unittest.main()
