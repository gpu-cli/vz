"""Offline health/protocol/ownership tests; no host sockets or VM execution."""
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import types
import unittest
from unittest.mock import Mock, patch

import linux_docker_parallel_health as health

TOKEN = 'vzcompose-owned-health'
EPOCH = 1_800_000_000_000_000_000
MONO = 10_000_000_000
INTERVALS = [(EPOCH + 2_000_000_000 + i, EPOCH + 50_000_000_000 + i) for i in range(4)]


def rows():
    result = [{'type': 'start', 'schema_version': 1, 'token': TOKEN, 'pid': 2,
               'unix_ns': EPOCH, 'monotonic_ns': MONO, 'timing': dict(health.TIMING)}]
    for index in range(60):
        offset = index * 1_000_000_000
        result.append({'type': 'sample', 'sequence': index, 'planned_monotonic_ns': MONO + offset,
                       'started_monotonic_ns': MONO + offset, 'finished_monotonic_ns': MONO + offset + 1000,
                       'started_unix_ns': EPOCH + offset, 'finished_unix_ns': EPOCH + offset + 1000,
                       'status': 200, 'body': TOKEN + '\n'})
    result.append({'type': 'end', 'samples': 60, 'monotonic_ns': MONO + 59_000_001_000,
                   'unix_ns': EPOCH + 59_000_001_000})
    return result


def encode(value):
    return b''.join(json.dumps(row, separators=(',', ':')).encode() + b'\n' for row in value)


class ProtocolTests(unittest.TestCase):
    def check(self, value=None, intervals=INTERVALS, stderr=b''):
        return health.validate(encode(rows() if value is None else value), stderr, TOKEN, health.TIMING, intervals)

    def test_exact_sixty_samples_cover_all_four_runs(self):
        proof = self.check()
        self.assertEqual(proof['samples'], 60)
        self.assertEqual(proof['missed_deadlines'], 0)
        self.assertEqual(proof['guest_run_envelopes'], [list(x) for x in INTERVALS])
        self.assertNotIn('run_intervals', proof)
        self.assertIn('NOT_SHIFTED_BUILDX_DISPLAY_TIME', proof['clock_basis'])
        self.assertIn('NOT_NETWORK_CONFORMANCE', proof['scope'])

    def test_failed_marker_status_schedule_duration_identity_and_clock_rejected(self):
        mutations = [(0, 'token', 'foreign'), (0, 'pid', True), (0, 'extra', 1),
                     (10, 'sequence', 4), (10, 'status', 500), (10, 'status', True),
                     (10, 'body', 'wrong\n'), (10, 'planned_monotonic_ns', MONO),
                     (10, 'started_monotonic_ns', MONO + 9_250_000_001),
                     (10, 'finished_monotonic_ns', MONO + 9_500_000_001),
                     (10, 'started_unix_ns', EPOCH + 8_000_000_000),
                     (10, 'finished_unix_ns', EPOCH + 10_000_000_000),
                     (-1, 'samples', 59), (-1, 'monotonic_ns', MONO + 71_000_000_000)]
        for index, key, value in mutations:
            with self.subTest(index=index, key=key):
                changed = rows()
                changed[index][key] = value
                with self.assertRaises(ValueError):
                    self.check(changed)

    def test_missing_duplicate_extra_and_partial_streams_rejected(self):
        raw = encode(rows())
        for invalid in (raw[:-1], raw + b'\n', raw + raw, raw.replace(b'"sequence":0', b'"sequence":0,"sequence":0'),
                        encode(rows()[1:]), b'x' * (health.LIMIT + 1)):
            with self.subTest(raw=invalid[:25]), self.assertRaises(ValueError):
                health.validate(invalid, b'', TOKEN, health.TIMING, INTERVALS)
        with self.assertRaises(ValueError):
            self.check(stderr=b'warning\n')

    def test_four_intervals_must_all_be_bracketed(self):
        for intervals in ([], INTERVALS[:3], INTERVALS + [INTERVALS[0]],
                          [(EPOCH, EPOCH + 3)] + INTERVALS[1:],
                          INTERVALS[:3] + [(EPOCH + 2, EPOCH + 60_000_000_000)],
                          [(True, EPOCH + 3)] * 4, [(EPOCH + 3, EPOCH + 2)] * 4):
            with self.subTest(intervals=intervals), self.assertRaises(ValueError):
                self.check(intervals=intervals)

    def test_plausible_display_interval_cannot_replace_out_of_coverage_guest_envelope(self):
        # A translated display interval happens to fit within the HTTP window.
        # Guest S=+5s,C=+50s and preserved D=55s yield [-5s,+60s], however:
        # the full conservative envelope is not covered and must be rejected.
        displayed = (EPOCH + 2_000_000_000, EPOCH + 57_000_000_000)
        self.check(intervals=[displayed] * 4)
        start, completed = EPOCH + 5_000_000_000, EPOCH + 50_000_000_000
        duration = displayed[1] - displayed[0]
        envelope = (completed - duration, start + duration)
        with self.assertRaisesRegex(ValueError, 'conservative guest RUN envelope'):
            self.check(intervals=[envelope] * 4)
        # A different authenticated solve S=+10s,C=+50s,D=48s yields [+2s,+58s].
        inside = (EPOCH + 2_000_000_000, EPOCH + 58_000_000_000)
        proof = self.check(intervals=[inside] * 4)
        self.assertEqual(proof['guest_run_envelopes'], [list(inside)] * 4)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        previous = os.umask(0o077)
        self.addCleanup(os.umask, previous)

    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        fixture = root / 'fixture'
        fixture.mkdir()
        (fixture / 'health.py').write_bytes(b'fixture source')
        (fixture / 'contract.json').write_text(json.dumps({'health': health.TIMING}))
        for path in fixture.iterdir():
            path.chmod(0o600)
        descriptor = {'name': 'owned-context', 'engine_id': 'owned-engine', 'endpoint': 'unix:///owned',
                      'owner': {'machine_id': 'owned-machine'}}
        image = 'sha256:' + 'a' * 64
        row = {'descriptor': descriptor, 'token': TOKEN, 'tag': TOKEN + ':fixture', 'image_id': image}
        h = types.SimpleNamespace(evidence=root, env={}, config=root / 'docker', root=root,
             info={'clients': {'docker': {'canonical': '/owned/docker'}}, 'parallel_fixture': str(fixture)}, owned=[row],
             exact_absent=Mock(), mutate=Mock(), docker=Mock())
        selected = health.Health(h, descriptor, {'compose': {'id': image}}, 0)
        selected.route = Mock()
        selected.container_id, selected.token = 'b' * 64, TOKEN
        command = ['-u', '-c', selected.source, 'serve']
        item = {'Id': selected.container_id, 'Name': '/' + TOKEN, 'Image': image, 'Path': 'python3', 'Args': command,
                'Config': {'Labels': {health.LABEL: TOKEN}, 'Entrypoint': ['python3'], 'Cmd': command},
                'HostConfig': {'NetworkMode': 'none', 'Runtime': 'youki'},
                'RestartCount': 0, 'State': {'Pid': 123, 'StartedAt': 'exact', 'Running': True, 'Status': 'running',
                                          'Paused': False, 'Restarting': False, 'Dead': False, 'OOMKilled': False}}
        return selected, h, row, item

    def ready(self):
        selected, h, row, item = self.fixture()
        selected.prepared, selected.before = True, item
        h.docker.return_value = (json.dumps([item]).encode(), b'', 0)
        original = selected.record.run
        def record_run(*args, **kwargs):
            with patch.object(health.startup, 'execute', return_value=subprocess.CompletedProcess([], 0, encode(rows()), b'')):
                return original(*args, **kwargs)
        selected.record.run = Mock(side_effect=record_run)
        return selected, h, row, item

    def test_dedicated_thread_positively_reaped_before_validation(self):
        selected, h, _, _ = self.ready()
        selected.start()
        proof = selected.finish(INTERVALS)
        self.assertFalse(selected.thread.is_alive())
        selected.record.run.assert_called_once()
        self.assertEqual(selected.record.run.call_args.args[0], 'http-health')
        self.assertEqual(proof['container_id'], selected.container_id)
        self.assertEqual(h.docker.call_count, 1)
        with self.assertRaises(ValueError):
            selected.start()

    def test_failure_path_still_joins_before_rejecting_missing_intervals(self):
        selected, h, _, _ = self.ready()
        selected.start()
        with self.assertRaisesRegex(ValueError, 'four authenticated'):
            selected.finish([])
        self.assertFalse(selected.thread.is_alive())
        h.docker.assert_not_called()
        self.assertFalse((selected.output / 'health-validation.json').exists())

    def test_capture_failure_never_produces_success_or_leaves_thread(self):
        for result in ('exception', 'uncertain', 'nonzero'):
            selected, h, _, _ = self.ready()
            if result == 'exception':
                selected.record.run.side_effect = TimeoutError('reaped observer deadline')
            elif result == 'uncertain':
                selected.record.run.side_effect = None
                selected.record.run.return_value = (encode(rows()), b'', 0)
                selected.record.receipts.append({'capture_complete': True, 'effects_uncertain': True})
            else:
                selected.record.run.side_effect = None
                selected.record.run.return_value = (b'', b'failed', 1)
            selected.start()
            with self.subTest(result=result), self.assertRaises(ValueError):
                selected.finish(INTERVALS)
            self.assertFalse(selected.thread.is_alive())
            h.docker.assert_not_called()

    def test_restart_or_foreign_image_rejected_after_capture(self):
        for key, value in (('Pid', 124), ('StartedAt', 'later')):
            selected, h, _, item = self.ready()
            changed = copy.deepcopy(item)
            changed['State'][key] = value
            h.docker.return_value = (json.dumps([changed]).encode(), b'', 0)
            selected.start()
            with self.assertRaisesRegex(ValueError, 'restarted or replaced'):
                selected.finish(INTERVALS)
        selected, h, _, item = self.ready()
        changed = copy.deepcopy(item)
        changed['Image'] = 'foreign'
        h.docker.return_value = (json.dumps([changed]).encode(), b'', 0)
        with self.assertRaises(ValueError):
            selected.inspect('check')

    def test_prepare_registers_container_on_existing_image_row_before_start(self):
        selected, h, row, item = self.fixture()
        h.mutate.return_value = ((selected.container_id + '\n').encode(), b'', 0)
        def mutation(label, *args):
            if label == 'health-container-start':
                self.assertEqual(row['container_id'], selected.container_id)
            return ((selected.container_id + '\n').encode(), b'', 0)
        h.mutate.side_effect = mutation
        h.docker.side_effect = [(b'', b'', 0), (json.dumps([item]).encode(), b'', 0)]
        selected.prepare()
        self.assertEqual(h.owned, [row])
        self.assertTrue(selected.prepared)
        args = h.mutate.call_args_list[0].args[2]
        self.assertEqual(args[:4], ['container', 'create', '--network', 'none'])
        self.assertNotIn('--publish', args)
        self.assertEqual(args[args.index('--entrypoint') + 1], 'python3')
        self.assertEqual(args[-5:], [selected.images['compose']['id'], '-u', '-c', selected.source, 'serve'])

    def test_inherited_compose_entrypoint_and_wrong_service_argv_rejected(self):
        for target, key, value in (('Config', 'Entrypoint', ['python3', '-u', '/fixture/service.py']),
                                   ('Config', 'Cmd', ['-u', '-c', 'foreign', 'serve']),
                                   ('top', 'Path', '/fixture/service.py'), ('top', 'Args', ['foreign'])):
            selected, h, _, item = self.fixture()
            changed = copy.deepcopy(item)
            (changed if target == 'top' else changed[target])[key] = value
            h.docker.return_value = (json.dumps([changed]).encode(), b'', 0)
            with self.subTest(key=key), self.assertRaises(ValueError):
                selected.inspect('service')

    def test_preexisting_or_foreign_image_row_rejects_before_mutation(self):
        for change in ('preexisting', 'foreign', 'duplicate'):
            selected, h, row, _ = self.fixture()
            if change == 'preexisting': row['container_id'] = 'existing'
            if change == 'foreign': row['descriptor'] = {'owner': 'foreign'}
            if change == 'duplicate': h.owned.append(copy.deepcopy(row))
            with self.subTest(change=change), self.assertRaises(ValueError):
                selected.prepare()
            h.mutate.assert_not_called()

    def test_selected_custom_fixture_drift_rejected_before_start(self):
        for name in ('health.py', 'contract.json'):
            selected, _, _, _ = self.ready()
            (selected.fixture / name).write_bytes(b'changed')
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'source/contract'):
                selected.start()
            selected.record.run.assert_not_called()

    def test_context_and_engine_reroutes_rejected_before_mutations(self):
        for foreign in ('context', 'engine'):
            selected, h, _, _ = self.fixture()
            context = {'Name': 'owned-context', 'Endpoints': {'docker': {'Host': 'unix:///owned'}}}
            if foreign == 'context': context['Endpoints']['docker']['Host'] = 'unix:///foreign'
            h.docker.side_effect = [(json.dumps([context]).encode(), b'', 0),
                                   (b'foreign' if foreign == 'engine' else b'owned-engine', b'', 0)]
            with self.subTest(foreign=foreign), self.assertRaises(ValueError):
                health.Health.route(selected)
            h.mutate.assert_not_called()

    def test_independent_record_rejects_forged_command_or_stream(self):
        for filename, key, value in (('001-http-health.intent.json', 'argv', ['docker', 'foreign']),
                                     ('001-http-health.result.json', 'exit_code', 1),
                                     ('001-http-health.result.json', 'capture_complete', False),
                                     ('observer-input.json', 'environment', {'DOCKER_HOST': 'foreign'})):
            selected, h, _, _ = self.ready()
            selected.start()
            selected.thread.join(2)
            path = selected.output / filename
            changed = json.loads(path.read_bytes())
            changed[key] = value
            path.write_text(json.dumps(changed))
            with self.subTest(filename=filename, key=key), self.assertRaises(ValueError):
                selected.finish(INTERVALS)
            h.docker.assert_not_called()
        selected, h, _, _ = self.ready()
        selected.start()
        selected.thread.join(2)
        (selected.output / '001-http-health.stdout').write_bytes(encode(rows()).replace(b'owned-health', b'other-health'))
        with self.assertRaisesRegex(ValueError, 'digest/size'):
            selected.finish(INTERVALS)
        h.docker.assert_not_called()


class GuestProbeTests(unittest.TestCase):
    def test_actual_probe_algorithm_sixty_loopback_requests_without_retry(self):
        spec = importlib.util.spec_from_file_location('parallel_health_fixture', health.FIXTURE / 'health.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        class Clock:
            current = MONO
            def monotonic_ns(self):
                self.current += 1000
                return self.current
            def time_ns(self): return EPOCH + self.current - MONO
            def sleep(self, seconds): self.current += round(seconds * 10**9)
        connection = Mock()
        connection.getresponse.return_value = types.SimpleNamespace(status=200, read=Mock(return_value=(TOKEN + '\n').encode()))
        with patch.object(module, 'time', Clock()), patch.object(module.http.client, 'HTTPConnection', return_value=connection) as client, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            module.probe(TOKEN, health.TIMING)
        self.assertEqual(client.call_count, 60)
        for call in client.call_args_list:
            self.assertEqual(call.args, ('127.0.0.1', 8080))
            self.assertEqual(call.kwargs, {'timeout': .5})
        proof = health.validate(output.getvalue().encode(), b'', TOKEN, health.TIMING, INTERVALS)
        self.assertEqual(proof['samples'], 60)
        connection.getresponse.return_value.status = 503
        with patch.object(module, 'time', Clock()), patch.object(module.http.client, 'HTTPConnection', return_value=connection) as client, \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, 'HTTP health'):
            module.probe(TOKEN, health.TIMING)
        self.assertEqual(client.call_count, 1)


if __name__ == '__main__':
    unittest.main()
