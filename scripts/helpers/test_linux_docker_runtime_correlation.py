"""Synthetic source-selected request adversaries; no Engine or runtime launch."""
import copy
import hashlib
import json
import unittest
from unittest import mock

import linux_docker_runtime_correlation as correlation

SESSION = 'a' * 64
BOOT = 'b1234567-1111-2222-3333-0123456789ab'
CID = 'c' * 64
FOREIGN = 'd' * 64
ENROLLMENT = json.dumps({'schema_version': 1, 'session_id': SESSION, 'boot_id': BOOT}).encode()


def pair(first=1, *, operation='exec', cid=CID, wall=100, pid=10, code=0, outcome='ok'):
    common = {'schema_version': 1, 'session_id': SESSION, 'boot_id': BOOT,
              'invocation_id': '%d:20:%d' % (pid, first * 10), 'operation': operation,
              'container_id': cid, 'pid': pid, 'starttime_ticks': 20}
    return [dict(common, sequence=first + offset, event=event,
                 monotonic_ns=first * 10 + offset, wall_time_ns=wall + offset,
                 outcome=None if offset == 0 else outcome, exit_code=None if offset == 0 else code)
            for offset, event in enumerate(('begin', 'result'))]


def pack(rows):
    return b''.join(json.dumps(row, separators=(',', ':')).encode() + b'\n' for row in rows)


def request(name='explicit-exec', *, docker='exec', runtime='exec', cid=CID, before=90, after=110):
    return {'request_id': name, 'container_id': cid, 'docker_operation': docker,
            'runtime_operation': runtime, 'engine_before_ns': before, 'engine_after_ns': after}


def correlate(rows=None, requests=None, **kwargs):
    options = {'enrollment_raw': ENROLLMENT, 'status_raw': b'complete\n',
               'expected_boot_id': BOOT, 'expected_session_id': SESSION,
               'requests': [request()] if requests is None else requests}
    options.update(kwargs)
    return correlation.correlate(pack(pair() if rows is None else rows), **options)


class CorrelationTests(unittest.TestCase):
    def test_unique_candidate_binds_raw_hash_and_preserves_failure_semantics(self):
        events = pair(code=255, outcome='error')
        proof = correlate(events)
        self.assertEqual(proof['journal_sha256'], hashlib.sha256(pack(events)).hexdigest())
        self.assertEqual(proof['matches'][0]['status'], 'unique')
        self.assertEqual(proof['matches'][0]['candidates'][0]['runtime_exit_code'], 255)
        self.assertEqual(proof['matches'][0]['candidates'][0]['outcome'], 'error')
        self.assertEqual(proof['unmapped_invocations'], [])
        for key in ('request_receipts_authenticated', 'engine_clock_binding_certified',
                    'docker_operation_mapping_certified', 'full_process_absence_certified',
                    'payload_exit_status_certified', 'historical_generation_identity_certified'):
            self.assertIs(proof[key], False)
        self.assertFalse(proof['matches'][0]['causal_attribution_certified'])

    def test_healthcheck_and_explicit_exec_remain_ambiguous(self):
        events = pair() + pair(3, wall=103, pid=11, code=37)
        proof = correlate(events)
        self.assertEqual(proof['matches'][0]['status'], 'ambiguous')
        self.assertEqual(len(proof['matches'][0]['candidates']), 2)
        self.assertFalse(proof['all_requested_relations_unique'])
        self.assertEqual([row['runtime_exit_code'] for row in proof['matches'][0]['candidates']], [0, 37])

    def test_foreign_and_other_operation_background_preserved(self):
        events = pair() + pair(3, wall=103, pid=11, cid=FOREIGN)
        events += pair(5, wall=106, pid=12, operation='features', cid=None)
        proof = correlate(events)
        self.assertEqual(proof['matches'][0]['status'], 'unique')
        self.assertEqual([row['container_id'] for row in proof['unmapped_invocations']], [FOREIGN, None])

    def test_exact_boot_session_and_cid_mismatch_reject(self):
        for kwargs in ({'expected_boot_id': BOOT.replace('b', 'c')}, {'expected_session_id': 'e' * 64},
                       {'requests': [request(cid=FOREIGN)]}):
            with self.assertRaises(ValueError):
                correlate(**kwargs)
        for value in ('c' * 12, CID.upper(), CID + '\n', True):
            with self.assertRaises(ValueError):
                correlate(requests=[request(cid=value)])

    def test_window_requires_complete_invocation_and_accepts_exact_boundaries(self):
        proof = correlate(requests=[request(before=100, after=101)])
        self.assertEqual(proof['matches'][0]['status'], 'unique')
        for before, after in ((101, 110), (90, 100), (110, 120), (1, 90)):
            with self.assertRaises(ValueError):
                correlate(requests=[request(before=before, after=after)])

    def test_invocation_cannot_serve_multiple_or_ambiguous_requests(self):
        for events in (pair(), pair() + pair(3, wall=103, pid=11)):
            with self.assertRaisesRegex(ValueError, 'reused'):
                correlate(events, requests=[request('one'), request('two')])

    def test_invalid_times_booleans_and_clock_regression(self):
        for before, after in ((True, 110), (90, 110.0), (0, 110), (110, 110), (111, 110),
                              (1, 2 + correlation.MAX_WINDOW_NS), (90, 1 << 64)):
            with self.assertRaises(ValueError):
                correlate(requests=[request(before=before, after=after)])
        events = pair() + pair(3, wall=99, pid=11)
        with self.assertRaisesRegex(ValueError, 'regressed'):
            correlate(events)

    def test_no_invented_calls_for_metadata_logs_wait_signal_and_removal(self):
        for operation in ('create', 'inspect', 'logs', 'wait', 'stop', 'kill', 'rm'):
            proof = correlate(requests=[request(docker=operation, runtime=None)])
            self.assertEqual(proof['matches'][0]['status'], 'not_applicable')
            self.assertEqual(len(proof['unmapped_invocations']), 1)
            with self.assertRaises(ValueError):
                correlate(requests=[request(docker=operation)])

    def test_runtime_task_relations_not_docker_exit_equivalence(self):
        for docker in ('start', 'run', 'restart'):
            for runtime in ('create', 'start'):
                events = pair(operation=runtime, code=1, outcome='error')
                proof = correlate(events, requests=[request(docker=docker, runtime=runtime)])
                self.assertEqual(proof['matches'][0]['candidates'][0]['runtime_exit_code'], 1)
            with self.assertRaises(ValueError):
                correlate(requests=[request(docker=docker, runtime='run')])

    def test_ordered_epochs_require_disjoint_external_windows(self):
        events = pair(operation='start') + pair(3, wall=200, pid=11, operation='start')
        requests = [request('first-generation', docker='start', runtime='start'),
                    request('second-generation', docker='restart', runtime='start', before=190, after=210)]
        proof = correlate(events, requests=requests)
        self.assertEqual([item['candidates'][0]['begin_sequence'] for item in proof['matches']], [1, 3])
        self.assertFalse(proof['historical_generation_identity_certified'])

    def test_requests_are_bounded_exact_schema_and_not_mutated(self):
        original = request()
        for requests in ([], [original] * 257, [original, original],
                         [dict(original, extra='raw-secret')], [dict(original, request_id='../escape')],
                         [dict(original, docker_operation='invented')],
                         [dict(original, runtime_operation=None)]):
            with self.assertRaises(ValueError):
                correlate(requests=requests)
        snapshot = copy.deepcopy(original)
        correlate(requests=[original])
        self.assertEqual(original, snapshot)

    def test_complete_raw_parser_still_required_and_no_dispatch_or_files(self):
        with self.assertRaises(ValueError):
            correlate(pair()[:1])
        with self.assertRaises(ValueError):
            correlate(status_raw=b'incomplete\n')
        with mock.patch('builtins.open', side_effect=AssertionError('file I/O')):
            self.assertEqual(correlate()['invocation_count'], 1)


if __name__ == '__main__':
    unittest.main()
