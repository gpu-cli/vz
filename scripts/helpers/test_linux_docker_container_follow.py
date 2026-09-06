"""Finite local children and inert follow mocks; no Docker/server dispatch."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import docker_host_driver as driver
import linux_docker_container_follow as subject
import linux_docker_interactive_capture as capture

CID, TOKEN = 'a'*64, 'vzio-'+'b'*24


class Progress(unittest.TestCase):
    def capture(self, callback, script='import os;os.write(1,b"public");os.write(2,b"ready")'):
        with tempfile.TemporaryDirectory(prefix='vz-follow-observer-') as root:
            result = capture.capture([sys.executable, '-c', script], executable=sys.executable,
                cwd=root, env={'PATH': '/usr/bin:/bin'}, plan={'schema_version': 1, 'mode': 'pipes',
                'timeout_seconds': 2, 'input_limit': 1, 'output_limit': 4096,
                'actions': [{'kind': 'close_stdin'}]}, progress_observer=callback)
            if result.pending_process is not None:
                result.pending_process.wait(timeout=3)
            return result

    def test_real_reads_counts_only_and_callback_cannot_mutate_receipt(self):
        observations = []
        def observer(row):
            observations.append(copy.deepcopy(row))
            row['observed_bytes']['stdout'] = -1
            row['observed']['unix_ns'] = -1
        result = self.capture(observer)
        self.assertTrue(result.receipt['capture_complete'])
        self.assertEqual(result.receipt['read_progress'], observations)
        self.assertEqual(observations[-1]['observed_bytes'], {'stdout': 6, 'stderr': 5, 'tty': 0})
        self.assertNotIn('public', json.dumps(observations))
        self.assertNotIn('ready', json.dumps(observations))

    def test_no_observer_keeps_original_receipt_shape(self):
        result = self.capture(None)
        self.assertTrue(result.receipt['capture_complete'])
        self.assertNotIn('read_progress', result.receipt)

    def test_observer_exception_fails_closed_without_echoing_error(self):
        def observer(row): raise RuntimeError('private-error-content')
        result = self.capture(observer)
        self.assertFalse(result.receipt['capture_complete'])
        self.assertTrue(result.receipt['effects_uncertain'])
        self.assertNotIn('private-error-content', json.dumps(result.receipt))

    def test_observation_cap_not_silent_truncation(self):
        with patch.object(capture, 'CHUNK', 1):
            result = self.capture(lambda row: None, 'import os;os.write(1,b"a"*2050)')
        self.assertFalse(result.receipt['capture_complete'])
        self.assertEqual(len(result.receipt['read_progress']), 2048)
        self.assertEqual(result.receipt['error'], 'progress_observation_limit')

    def test_recorder_forwards_hook_to_real_finite_capture(self):
        with tempfile.TemporaryDirectory(prefix='vz-follow-recorder-') as root:
            item = driver.Recorder(Path(root).resolve(), {'PATH': '/usr/bin:/bin'}, [], max_stream_bytes=4096)
            seen = []
            command = item.run([sys.executable, '-c', 'print("ready")'], executable=sys.executable,
                timeout=2, mutation=False, interaction_plan={'schema_version': 1, 'mode': 'pipes',
                'timeout_seconds': 2, 'input_limit': 1, 'output_limit': 4096,
                'actions': [{'kind': 'close_stdin'}]}, progress_observer=seen.append)
            receipt = json.loads((Path(root)/('command-%05d.json' % command.index)).read_bytes())
            self.assertEqual(receipt['interaction_capture']['read_progress'], seen)
            self.assertEqual(command.returncode, 0)

    def test_observer_without_capture_rejected_before_dispatch(self):
        with tempfile.TemporaryDirectory(prefix='vz-follow-invalid-') as root:
            item = driver.Recorder(Path(root), {}, [])
            with self.assertRaises(ValueError):
                item.run(['false'], executable='/usr/bin/false', progress_observer=lambda row: None)
            self.assertEqual(item.count, 0)


class PrefixReplay(unittest.TestCase):
    def setUp(self):
        _, _, self.ready = subject.specification(CID, TOKEN)
        self.stdout = self.ready['stdout'] + b'final\n'
        self.stderr = self.ready['stderr']
        self.capture = {'started': {'unix_ns': 100, 'monotonic_ns': 1000},
                        'completed': {'unix_ns': 200, 'monotonic_ns': 1100}, 'read_progress': [
            {'index': 0, 'stream': 'stdout', 'observed_bytes': {'stdout': len(self.ready['stdout']), 'stderr': 0, 'tty': 0},
             'observed': {'unix_ns': 120, 'monotonic_ns': 1020}},
            {'index': 1, 'stream': 'stderr', 'observed_bytes': {'stdout': len(self.ready['stdout']), 'stderr': len(self.stderr), 'tty': 0},
             'observed': {'unix_ns': 130, 'monotonic_ns': 1030}},
            {'index': 2, 'stream': 'stdout', 'observed_bytes': {'stdout': len(self.stdout), 'stderr': len(self.stderr), 'tty': 0},
             'observed': {'unix_ns': 170, 'monotonic_ns': 1070}}]}

    def verify(self, termination=150):
        return subject.replay_progress(self.capture, self.stdout, self.stderr, self.ready, termination)

    def test_exact_both_prefixes_observed_before_external_term(self):
        self.assertEqual(self.verify(), self.capture['read_progress'][1])

    def test_early_equal_or_unbound_term_rejected(self):
        for value in (129, 130, 201, True, None):
            with self.subTest(value=value), self.assertRaises(ValueError): self.verify(value)

    def test_progress_missing_regressed_foreign_or_unobserved_rejected(self):
        original = copy.deepcopy(self.capture)
        for key in ('missing', 'reorder', 'both-changed', 'unobserved', 'bool', 'clock', 'unknown'):
            self.capture = copy.deepcopy(original)
            rows = self.capture['read_progress']
            if key == 'missing': rows.pop()
            elif key == 'reorder': rows.reverse()
            elif key == 'both-changed': rows[0]['observed_bytes']['stderr'] = 1
            elif key == 'unobserved': rows[1]['observed_bytes']['stderr'] += 1
            elif key == 'bool': rows[0]['index'] = False
            elif key == 'clock': rows[1]['observed']['monotonic_ns'] = 100
            else: rows[0]['private'] = 'unexpected'
            with self.subTest(key=key), self.assertRaises(ValueError): self.verify()

    def test_noncanonical_initial_stream_cannot_pass_counts(self):
        self.stdout = b'x' + self.stdout[1:]
        with self.assertRaises(ValueError): self.verify()


class Orchestration(unittest.TestCase):
    def test_failed_follower_retained_without_term_or_relaunch(self):
        with tempfile.TemporaryDirectory(prefix='vz-follow-failed-mock-') as root:
            root = Path(root).resolve()
            registered, termination = [], []
            item = SimpleNamespace(inputs=None, fixture=root, output=root, guard=lambda: None)
            follower = SimpleNamespace(output=root/'follow', guard=lambda: None)
            follower.output.mkdir()
            follower.record = SimpleNamespace(count=0, max_stream_bytes=subject.LIMIT, pending_interactions=['owned-pending'],
                persist=lambda path, value, **kwargs: path.write_text(json.dumps(value)))
            error = ValueError('capture failed')
            def command(*args, **kwargs): raise error
            follower.command = command
            with patch.object(subject, 'Driver', return_value=follower), self.assertRaises(ValueError):
                subject.run_follow(item, CID, TOKEN, service_guard=lambda *args: None,
                    terminate=lambda: termination.append(True), register_follower=registered.append)
            self.assertEqual(registered, [follower])
            self.assertEqual(termination, [])
            self.assertIs(follower.follow_state['error'], error)
            self.assertFalse(follower.follow_thread.is_alive())
            retained = json.loads((follower.output/'follow-disposition.json').read_bytes())
            self.assertEqual(retained['pending_interactions'], 1)
            self.assertEqual(retained['capture_error_type'], 'ValueError')

    def test_registered_before_dispatch_and_term_after_both_stream_reads(self):
        with tempfile.TemporaryDirectory(prefix='vz-follow-mock-') as root:
            root = Path(root).resolve()
            events, registered = [], []
            terminated = threading.Event()
            _, _, ready = subject.specification(CID, TOKEN)
            raw = {'docker_config': '/private/config', 'clients': {'docker': {'path': '/pinned/docker'}},
                   'scope': {'docker_context': 'owned-machine'}}
            item = SimpleNamespace(inputs=SimpleNamespace(raw=raw, scope=raw['scope']), fixture=root, output=root,
                                   guard=lambda: events.append('main-guard'))
            follower = SimpleNamespace(output=root/'follow', env={}, guard=lambda: events.append('follow-guard'))
            follower.output.mkdir()
            def persist(path, value, **kwargs): path.write_text(json.dumps(value))
            follower.record = SimpleNamespace(count=0, max_stream_bytes=subject.LIMIT,
                                               persist=persist, pending_interactions=[])
            def command(args, **kwargs):
                self.assertTrue(registered)
                events.append('dispatch')
                callback = kwargs['progress_observer']
                callback({'index': 0, 'stream': 'stdout', 'observed_bytes': {'stdout': len(ready['stdout']), 'stderr': 0, 'tty': 0},
                          'observed': {'unix_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns()}})
                self.assertFalse(terminated.is_set())
                callback({'index': 1, 'stream': 'stderr', 'observed_bytes': {'stdout': len(ready['stdout']), 'stderr': len(ready['stderr']), 'tty': 0},
                          'observed': {'unix_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns()}})
                self.assertTrue(terminated.wait(3))
                return SimpleNamespace(index=1, stdout=b'', stderr=b'')
            follower.command = command
            def terminate():
                events.append('TERM'); terminated.set()
                self.assertTrue((follower.output/'follow-ready.json').exists())
                return {'command_index': 7, 'started_unix_ns': time.time_ns()}
            with patch.object(subject, 'Driver', return_value=follower), \
                    patch.object(subject.interactive, 'validate_recorded', return_value={}), \
                    patch.object(subject.fixture, 'validate_service', return_value={}), \
                    patch.object(subject, 'replay_follow', side_effect=lambda out, inp, cid, tok, proof, **kw: proof):
                proof = subject.run_follow(item, CID, TOKEN, service_guard=lambda *args: events.append('service-guard'),
                                           terminate=terminate, register_follower=registered.append)
            self.assertEqual(proof['container_id'], CID)
            self.assertEqual(registered, [follower])
            self.assertFalse(follower.follow_thread.is_alive())
            self.assertLess(events.index('dispatch'), events.index('TERM'))


if __name__ == '__main__':
    unittest.main()
