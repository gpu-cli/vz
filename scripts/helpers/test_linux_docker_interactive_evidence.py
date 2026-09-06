"""Independent replay adversaries using finite local captures, not Docker proof."""
import copy
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest

import linux_docker_interactive_capture as capture
import linux_docker_interactive_evidence as evidence
from test_linux_docker_interactive_capture import QUEUED_CHILD, queued_plan


class Evidence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='vz-interactive-replay-')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.fixtures = {}
        pipes = 'import os,sys;os.write(2,b"READY\\n");sys.stdout.buffer.write(sys.stdin.buffer.read());sys.exit(37)'
        pty = '''import os,termios,tty,signal,fcntl,struct
original=termios.tcgetattr(0)
def resized(a,b):
 assert struct.unpack('HHHH',fcntl.ioctl(0,termios.TIOCGWINSZ,b'\\0'*8))[:2]==(40,120)
 os.write(1,b'SIZED')
signal.signal(signal.SIGWINCH,resized)
try:
 tty.setraw(0)
 os.write(1,b'READY')
 assert os.read(0,1)==b'a'
 os.write(1,b'DATA')
 assert os.read(0,1)==b'q'
finally:termios.tcsetattr(0,termios.TCSANOW,original)
os.write(2,b'DONE')
'''
        for mode, script, actions, expected in (
            ('pipes', pipes, [
                {'kind': 'write', 'data': b'a', 'after': {'stream': 'stderr', 'marker': b'READY\n'}},
                {'kind': 'close_stdin'}], 37),
            ('pty', pty, [
                {'kind': 'write', 'data': b'a', 'after': {'stream': 'tty', 'marker': b'READY'}},
                {'kind': 'resize', 'rows': 40, 'cols': 120, 'after': {'stream': 'tty', 'marker': b'DATA'}},
                {'kind': 'write', 'data': b'q', 'after': {'stream': 'tty', 'marker': b'SIZED'}}], 0)):
            plan = {'schema_version': 1, 'mode': mode, 'timeout_seconds': 1,
                    'input_limit': 1 if mode == 'pipes' else 2, 'output_limit': 1024, 'actions': actions}
            arguments = {'argv': [sys.executable, '-c', script], 'executable': sys.executable,
                         'cwd': cls.temporary.name, 'env': {'PATH': '/usr/bin:/bin'}}
            result = capture.capture(**arguments, plan=plan)
            if result.pending_process is not None:
                result.pending_process.wait(timeout=3)
            if not result.receipt['capture_complete']:
                raise AssertionError('finite capture failed: ' + str(result.receipt['error']))
            cls.fixtures[mode] = {'plan_raw': evidence.encode_plan(plan), 'receipt': result.receipt,
                'stdout': result.stdout, 'stderr': result.stderr, **arguments, 'expected_exit': expected}
        arguments = {'argv': [sys.executable, '-c', QUEUED_CHILD], 'executable': sys.executable,
                     'cwd': cls.temporary.name, 'env': {'PATH': '/usr/bin:/bin'}}
        result = capture.capture(**arguments, plan=queued_plan())
        if result.pending_process is not None:
            result.pending_process.wait(timeout=3)
        if not result.receipt['capture_complete']:
            raise AssertionError('deferred capture failed: ' + str(result.receipt['error']))
        cls.fixtures['deferred'] = {'plan_raw': evidence.encode_plan(queued_plan()), 'receipt': result.receipt,
            'stdout': result.stdout, 'stderr': result.stderr, **arguments, 'expected_exit': 0}

    def fixture(self, mode='pipes'):
        return copy.deepcopy(self.fixtures[mode])

    def verify(self, row):
        return evidence.validate_capture(**row)

    def reject(self, row):
        with self.assertRaises(ValueError):
            self.verify(row)

    def test_real_pipe_capture_and_public_plan_round_trip(self):
        row = self.fixture()
        proof = self.verify(row)
        self.assertEqual(row['stdout'], b'a')
        self.assertEqual(row['stderr'], b'READY\n')
        self.assertEqual(proof['exit_code'], 37)
        self.assertEqual(proof['stdin_eof_count'], 1)
        self.assertEqual(proof['action_count'], 2)
        self.assertIn('not_docker_semantics', proof['scope'])
        self.assertEqual(evidence.encode_plan(evidence.decode_plan(row['plan_raw'])), row['plan_raw'])

    def test_real_pty_capture_resize_and_restoration(self):
        row = self.fixture('pty')
        proof = self.verify(row)
        self.assertEqual(row['stdout'], b'READYDATASIZEDDONE')
        self.assertEqual(row['stderr'], b'')
        self.assertEqual(proof['mode'], 'pty')
        self.assertTrue(row['receipt']['terminal']['client_restored_attributes'])

    def test_deferred_real_capture_replays_mask_union_and_launch_timing(self):
        row = self.fixture('deferred')
        self.assertEqual(self.verify(row)['exit_code'], 0)
        self.assertEqual(row['stdout'], b'READYQUEUED_ONCE')
        self.assertEqual(evidence.encode_plan(evidence.decode_plan(row['plan_raw'])), row['plan_raw'])

    def test_deferred_mask_missing_foreign_unrestored_or_malformed_rejected(self):
        for mutation in ('missing', 'extra', 'signal', 'bool-signal', 'scope', 'unrestored',
                         'no-winch', 'extra-signal', 'after', 'duplicate', 'bool-mask', 'out-of-range'):
            row = self.fixture('deferred'); launch = row['receipt']['deferred_sigwinch']
            if mutation == 'missing': del row['receipt']['deferred_sigwinch']
            elif mutation == 'extra': launch['extra'] = True
            elif mutation == 'signal': launch['signal'] = 'SIGINT'
            elif mutation == 'bool-signal': launch['signal_number'] = True
            elif mutation == 'scope': launch['scope'] = 'all_parent_threads'
            elif mutation == 'unrestored': launch['restored'] = False
            elif mutation == 'no-winch': launch['child_inherited'].remove(int(signal.SIGWINCH))
            elif mutation == 'extra-signal':
                launch['child_inherited'] = sorted(set(launch['child_inherited']) | {int(signal.SIGUSR2)})
            elif mutation == 'after': launch['after'] = launch['child_inherited']
            elif mutation == 'duplicate': launch['child_inherited'] *= 2
            elif mutation == 'bool-mask': launch['before'] = [True]
            else: launch['before'] = [signal.NSIG]
            with self.subTest(mutation=mutation): self.reject(row)

    def test_deferred_launch_time_outside_capture_or_after_first_action_rejected(self):
        for mutation in ('before', 'after', 'reversed', 'wall-drift', 'late-restore'):
            row = self.fixture('deferred'); receipt = row['receipt']; launch = receipt['deferred_sigwinch']
            if mutation == 'before': launch['started']['monotonic_ns'] = receipt['started']['monotonic_ns'] - 1
            elif mutation == 'after': launch['completed']['monotonic_ns'] = receipt['completed']['monotonic_ns'] + 1
            elif mutation == 'reversed':
                launch['spawn_completed']['monotonic_ns'] = launch['spawn_started']['monotonic_ns'] - 1
            elif mutation == 'wall-drift': launch['spawn_started']['unix_ns'] += 2 * evidence.CLOCK_TOLERANCE_NS
            else:
                launch['completed'] = dict(receipt['actions'][0]['triggered'])
                launch['completed']['monotonic_ns'] += 1
            with self.subTest(mutation=mutation): self.reject(row)

    def test_deferred_plan_scope_and_receipt_cannot_be_silently_added_or_removed(self):
        row = self.fixture('pty')
        row['receipt']['deferred_sigwinch'] = self.fixture('deferred')['receipt']['deferred_sigwinch']
        self.reject(row)
        row = self.fixture('deferred'); plan = evidence.decode_plan(row['plan_raw'])
        del plan['defer_sigwinch']; row['plan_raw'] = evidence.encode_plan(plan)
        self.reject(row)
        for selected in (evidence.decode_plan(self.fixture()['plan_raw']) | {'defer_sigwinch': True},
                         queued_plan() | {'actions': []},
                         *(queued_plan() | {'defer_sigwinch': value} for value in (False, 1, None, 'true'))):
            with self.subTest(plan=selected), self.assertRaises(ValueError): evidence.encode_plan(selected)

    def test_raw_output_digest_and_stream_exchange_rejected(self):
        for mutation in ('stdout', 'stderr', 'digest', 'size', 'stream-swap'):
            row = self.fixture()
            if mutation in ('stdout', 'stderr'): row[mutation] += b'x'
            elif mutation == 'digest': row['receipt']['outputs']['stdout']['sha256'] = '0' * 64
            elif mutation == 'size': row['receipt']['outputs']['stdout']['size'] += 1
            else: row['stdout'], row['stderr'] = row['stderr'], row['stdout']
            with self.subTest(mutation=mutation): self.reject(row)

    def test_marker_hash_offset_and_unobserved_prefix_rejected(self):
        for mutation in ('hash', 'start', 'end', 'not-yet-seen', 'foreign-stream'):
            row = self.fixture(); action = row['receipt']['actions'][0]
            if mutation == 'hash': action['trigger']['marker_sha256'] = '0' * 64
            elif mutation == 'start': action['trigger']['start_offset'] += 1
            elif mutation == 'end': action['trigger']['end_offset'] += 1
            elif mutation == 'not-yet-seen': action['observed_bytes']['stderr'] = 5
            else: action['trigger']['stream'] = 'stdout'
            with self.subTest(mutation=mutation): self.reject(row)

    def test_actions_missing_duplicate_reordered_or_incomplete_rejected(self):
        for mutation in ('missing', 'duplicate', 'reordered', 'incomplete', 'index'):
            row = self.fixture(); actions = row['receipt']['actions']
            if mutation == 'missing': actions.pop()
            elif mutation == 'duplicate': actions.append(copy.deepcopy(actions[-1]))
            elif mutation == 'reordered': actions.reverse()
            elif mutation == 'incomplete': actions[0]['complete'] = False
            else: actions[0]['index'] = 1
            with self.subTest(mutation=mutation): self.reject(row)

    def test_action_timeline_and_prefix_regression_rejected(self):
        for mutation in ('before-start', 'overlap', 'after-end', 'negative', 'prefix-regression'):
            row = self.fixture(); receipt = row['receipt']; actions = receipt['actions']
            if mutation == 'before-start': actions[0]['triggered']['monotonic_ns'] = receipt['started']['monotonic_ns'] - 1
            elif mutation == 'overlap': actions[1]['triggered']['monotonic_ns'] = actions[0]['completed']['monotonic_ns'] - 1
            elif mutation == 'after-end': actions[1]['completed']['monotonic_ns'] = receipt['completed']['monotonic_ns'] + 1
            elif mutation == 'negative': actions[0]['triggered']['unix_ns'] = 0
            else: actions[1]['observed_bytes']['stderr'] = 0
            with self.subTest(mutation=mutation): self.reject(row)

    def test_capture_wall_clock_reversal_or_future_drift_rejected(self):
        for mutation in ('reversed-end', 'future-end', 'future-start'):
            row = self.fixture(); receipt = row['receipt']
            if mutation == 'reversed-end':
                receipt['completed']['unix_ns'] = receipt['started']['unix_ns'] - 1
            elif mutation == 'future-end':
                receipt['completed']['unix_ns'] += 2 * evidence.CLOCK_TOLERANCE_NS
            else:
                receipt['started']['unix_ns'] = receipt['completed']['unix_ns'] + 1
            # Only wall observations change; the original monotonic timeline,
            # raw streams, complete actions and process outcome remain valid.
            with self.subTest(mutation=mutation): self.reject(row)

    def test_action_wall_clock_reversed_future_or_outside_capture_rejected(self):
        for mutation in ('before-start', 'future-trigger', 'future-complete', 'reversed-action', 'reversed-order'):
            row = self.fixture(); receipt = row['receipt']; first, second = receipt['actions']
            if mutation == 'before-start':
                first['triggered']['unix_ns'] = receipt['started']['unix_ns'] - 1
            elif mutation == 'future-trigger':
                first['triggered']['unix_ns'] = receipt['completed']['unix_ns'] + 2 * evidence.CLOCK_TOLERANCE_NS
            elif mutation == 'future-complete':
                second['completed']['unix_ns'] = receipt['completed']['unix_ns'] + 1
            elif mutation == 'reversed-action':
                first['completed']['unix_ns'] = first['triggered']['unix_ns'] - 1
            else:
                second['triggered']['unix_ns'] = first['completed']['unix_ns'] - 1
            with self.subTest(mutation=mutation): self.reject(row)

    def test_capture_relative_wall_time_alone_does_not_claim_external_anchor(self):
        row = self.fixture()
        for observation in (row['receipt']['started'], row['receipt']['completed'],
                            *(stamp for action in row['receipt']['actions'] for stamp in
                              (action['triggered'], action['completed']))):
            observation['unix_ns'] += 10 * evidence.CLOCK_TOLERANCE_NS
        # Relative capture validation cannot anchor its entire clock. The
        # recorded-command tests separately reject this coherent shifted tuple
        # against the recorder's pre-dispatch wall/monotonic anchor.
        self.assertEqual(self.verify(row)['exit_code'], 37)

    def test_partial_write_wrong_data_or_duplicate_eof_rejected(self):
        for mutation in ('partial', 'hash', 'input-size', 'no-eof', 'duplicate-eof'):
            row = self.fixture(); action = row['receipt']['actions'][0]
            if mutation == 'partial': action['written_bytes'] = 0
            elif mutation == 'hash': action['input_sha256'] = '0' * 64
            elif mutation == 'input-size': action['input_size'] += 1
            elif mutation == 'no-eof': row['receipt']['stdin_eof_count'] = 0
            else: row['receipt']['stdin_eof_count'] = 2
            with self.subTest(mutation=mutation): self.reject(row)

    def test_foreign_invocation_environment_or_process_identity_rejected(self):
        for field, value in [('argv', ['/foreign']), ('executable', '/foreign'), ('cwd', '/foreign'),
                             ('env', {'HOME': '/foreign'})]:
            row = self.fixture(); row[field] = value
            with self.subTest(field=field): self.reject(row)
        for field in ('pid', 'process_group', 'session_id'):
            row = self.fixture(); row['receipt'][field] += 1
            with self.subTest(field=field): self.reject(row)

    def test_unreaped_uncertain_failed_or_terminated_capture_rejected(self):
        for field, value in [('owned_process_reaped', False), ('effects_uncertain', True),
                             ('capture_complete', False), ('error', 'deadline'),
                             ('cleanup_error', 'PermissionError'), ('termination', {'signal': 'SIGKILL'}),
                             ('returncode', 1), ('owned_direct_child', False)]:
            row = self.fixture(); row['receipt'][field] = value
            with self.subTest(field=field): self.reject(row)

    def test_normal_docker_acceptance_rejects_negative_host_signal_status(self):
        for code in (-signal.SIGINT, -signal.SIGTERM, -signal.SIGKILL):
            row = self.fixture(); row['expected_exit'] = row['receipt']['returncode'] = code
            with self.subTest(code=code): self.reject(row)

    def test_repaired_or_mismatched_terminal_never_passes(self):
        for mutation in ('repair', 'client-not-restored', 'restoration-failed', 'child-attributes',
                         'recovery-attributes', 'initial-size', 'controlling-terminal', 'stderr'):
            row = self.fixture('pty'); terminal = row['receipt']['terminal']
            if mutation == 'repair': terminal['repaired_by_harness'] = True
            elif mutation == 'client-not-restored': terminal['client_restored_attributes'] = False
            elif mutation == 'restoration-failed': terminal['restored_verified'] = False
            elif mutation == 'child-attributes': terminal['child_exited_termios'][0] ^= 1
            elif mutation == 'recovery-attributes': terminal['restored_attributes'][0] ^= 1
            elif mutation == 'initial-size': terminal['initial_size'] = [40, 120]
            elif mutation == 'controlling-terminal': terminal['controlling_terminal_claimed'] = True
            else: row['stderr'] = b'not-separate'
            with self.subTest(mutation=mutation): self.reject(row)

    def test_resize_signal_scope_or_dimensions_rejected(self):
        for field, value in [('rows', 41), ('cols', 121), ('signal', 'SIGTERM'), ('signal_scope', 'foreign')]:
            row = self.fixture('pty'); row['receipt']['actions'][1][field] = value
            with self.subTest(field=field): self.reject(row)

    def test_real_owned_sigterm_action_with_normal_exit_is_replayable(self):
        row = self.fixture()
        selected = evidence.decode_plan(row['plan_raw'])
        selected['actions'] = [{'kind': 'signal', 'name': 'SIGTERM',
                                'after': {'stream': 'stdout', 'marker': b'READY'}}]
        row['argv'] = [sys.executable, '-c', 'import os,signal,time;'
            'signal.signal(signal.SIGTERM,lambda a,b:os._exit(37));os.write(1,b"READY");time.sleep(2)']
        captured = capture.capture(row['argv'], executable=row['executable'], cwd=row['cwd'],
                                   env=row['env'], plan=selected)
        if captured.pending_process is not None:
            captured.pending_process.wait(timeout=3)
        self.assertTrue(captured.receipt['capture_complete'])
        row.update(plan_raw=evidence.encode_plan(selected), receipt=captured.receipt,
                   stdout=captured.stdout, stderr=captured.stderr)
        self.assertEqual(self.verify(row)['exit_code'], 37)
        row['receipt']['actions'][0]['signal_scope'] = 'unrelated-process'
        self.reject(row)

    def test_boolean_receipt_scalars_do_not_alias_integer_bindings(self):
        for path in [('schema_version',), ('timeout_seconds',), ('input_limit',),
                     ('actions', 0, 'written_bytes'), ('actions', 0, 'input_size'),
                     ('outputs', 'stdout', 'size')]:
            row = self.fixture(); parent = row['receipt']
            for key in path[:-1]: parent = parent[key]
            parent[path[-1]] = True
            with self.subTest(path=path): self.reject(row)

    def test_plan_tamper_digest_base64_and_unsafe_actions_rejected(self):
        encoded = json.loads(self.fixture()['plan_raw'])
        for mutation in ('digest', 'size', 'base64', 'eof-then-write', 'pty-eof', 'signal', 'input-bound'):
            row = copy.deepcopy(encoded)
            if mutation == 'digest': row['actions'][0]['data']['sha256'] = '0' * 64
            elif mutation == 'size': row['actions'][0]['data']['bytes'] = 2
            elif mutation == 'base64': row['actions'][0]['data']['value'] = '**'
            elif mutation == 'eof-then-write': row['actions'].reverse()
            elif mutation == 'pty-eof': row['mode'] = 'pty'
            elif mutation == 'signal': row['actions'] = [{'kind': 'signal', 'name': 'SIGKILL'}]
            else: row['input_limit'] = 0
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                evidence.decode_plan(evidence.canonical(row))

    def test_duplicate_nonfinite_and_oversized_json_rejected(self):
        for raw in (b'{"mode":"pipes","mode":"pty"}', b'{"value":NaN}',
                    b' ' * (evidence.MAX_PLAN_BYTES + 1)):
            with self.assertRaises(ValueError): evidence.parse(raw)


if __name__ == '__main__':
    unittest.main()
