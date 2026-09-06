"""Inert source-program/replay checks; dispatch and process creation prohibited."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_container_commands as commands
import linux_docker_container_fixture as fixture
import linux_docker_container_lifecycle as lane

CID = 'a' * 64
IMAGE = 'sha256:' + 'b' * 64
TOKEN = 'vzio-' + 'c' * 24


def inputs():
    return {'run_id': 'run-one', 'docker_config': '/owned/config',
            'clients': {'docker': {'path': '/owned/docker', 'sha256': 'd'*64}},
            'scope': {'docker_context': 'machine-context', 'docker_endpoint': 'unix:///owned/docker.sock',
                      'engine_id': 'engine-one'},
            'images': {'base': {'reference': fixture.BASE}}}


def bare():
    value = object.__new__(lane.Lifecycle)
    value.token, value.image_id, value.containers = TOKEN, IMAGE, {}
    value.inputs = SimpleNamespace(raw=inputs(), scope=inputs()['scope'])
    value.steps = []
    value.record = SimpleNamespace(receipts=[], acknowledge_negative=Mock())
    value.workload_complete = False
    value.follower = None
    return value


class SourcePlanTests(unittest.TestCase):
    def test_fixture_build_and_token_bind_explicit_scope(self):
        data = inputs()
        argv = lane.build_arguments(data, Path('/owned/fixture'), 'owned:tag')
        self.assertEqual(argv, ['buildx', 'build', '--builder', 'machine-context', '--platform', 'linux/arm64',
            '--network', 'none', '--progress', 'plain', '--load', '--no-cache', '--pull=false',
            '--build-arg', 'FIXTURE_BASE='+fixture.BASE, '--tag', 'owned:tag', '/owned/fixture'])
        original = lane.token(data)
        data['scope']['engine_id'] = 'other'
        self.assertNotEqual(original, lane.token(data))

    def test_attach_create_flags_and_registered_before_side_effect(self):
        value = bare(); value.absent = Mock(); value.inspect = Mock(return_value={'created': True})
        calls = []
        def step(label, args, **kwargs):
            self.assertIn('attach', value.containers)
            self.assertIsNone(value.containers['attach']['cid'])
            calls.append((label, args))
            return SimpleNamespace(stdout=(CID+'\n').encode(), stderr=b'')
        value.step = step
        result = value.create('attach', ['-u', '-c', lane.ATTACH_START, TOKEN],
                              interactive_input=True, entrypoint='python3')
        argv = calls[0][1]
        self.assertEqual(argv[argv.index('--interactive'):argv.index('--entrypoint')],
                         ['--interactive', '--attach', 'stdin', '--attach', 'stdout', '--attach', 'stderr'])
        self.assertEqual(argv[-7:], ['--entrypoint', 'python3', IMAGE, '-u', '-c', lane.ATTACH_START, TOKEN])
        self.assertEqual(result['cid'], CID)
        self.assertEqual(result['entrypoint'], ['python3'])
        with self.assertRaises(ValueError):
            value.create('attach', ['exit', '0'])

    def test_partial_create_retains_owned_unresolved_row(self):
        value = bare(); value.absent = Mock(); value.inspect = Mock()
        value.step = Mock(side_effect=RuntimeError('uncertain dispatch'))
        with self.assertRaises(RuntimeError):
            value.create('service', ['service', TOKEN])
        self.assertIn('service', value.containers)
        self.assertIsNone(value.containers['service']['cid'])
        value.inspect.assert_not_called()

    def test_attach_kickoff_marker_binary_eof_source_plan(self):
        value = bare()
        value.create = Mock(return_value={'cid': CID, 'created': {'Config': {
            key: True for key in ('StdinOnce', 'AttachStdin', 'AttachStdout', 'AttachStderr')}}})
        value.inspect = Mock(return_value={'State': {'Status': 'exited', 'Running': False, 'Pid': 0,
            'ExitCode': 37, 'StartedAt': '2026-09-06T12:00:00Z', 'FinishedAt': '2026-09-06T12:00:01Z'}})
        stdout = fixture.marker(TOKEN, 'stdout-begin') + fixture.INPUT + b'\n' + fixture.marker(TOKEN, 'stdout-end')
        stderr = fixture.marker(TOKEN, 'stderr-begin') + fixture.marker(TOKEN, 'stderr-end')
        value.step = Mock(return_value=SimpleNamespace(stdout=stdout, stderr=stderr, index=3))
        value.verify_interaction = Mock(return_value={'delegated': True})
        value.attach()
        label, args = value.step.call_args.args
        plan = value.step.call_args.kwargs['plan']
        self.assertEqual((label, args), ('attach-stream', ['attach', CID]))
        self.assertEqual(plan, lane.io_plan([{'kind': 'write', 'data': b'!'},
            {'kind': 'write', 'data': fixture.INPUT,
             'after': {'stream': 'stderr', 'marker': fixture.marker(TOKEN, 'stderr-begin')}},
            {'kind': 'close_stdin'}]))
        value.record.acknowledge_negative.assert_called_once()
        for key in ('StdinOnce', 'AttachStdin', 'AttachStdout', 'AttachStderr'):
            value.create.return_value['created']['Config'][key] = False
            value.step.reset_mock()
            with self.subTest(flag=key), self.assertRaises(ValueError):
                value.attach()
            value.step.assert_not_called()
            value.create.return_value['created']['Config'][key] = True

    def test_run_tty_uses_real_ctrl_c_plan_and_observed_signal(self):
        value = bare(); value.absent = Mock(); value.verify_interaction = Mock(return_value={'delegated': True})
        records = [{'schema_version': 1, 'type': 'tty_ready', 'token': TOKEN,
                    'isatty': [True, True, True], 'rows': 24, 'cols': 80},
                   {'schema_version': 1, 'type': 'observed_signal', 'token': TOKEN,
                    'signal': 'SIGINT', 'exit_code': 130}]
        stdout = b''.join(fixture.encode(row)+b'\r\n' for row in records)
        value.step = Mock(side_effect=[SimpleNamespace(stdout=stdout, stderr=b'', index=1),
                         SimpleNamespace(stdout=json.dumps([{'Id': CID}]).encode(), stderr=b'', index=2)])
        value.inspect = Mock(return_value={'State': {'Status': 'exited', 'Running': False, 'Pid': 0,
            'ExitCode': 130, 'StartedAt': '2026-09-06T12:00:00Z', 'FinishedAt': '2026-09-06T12:00:01Z'}})
        plan = {'schema_version': 1, 'mode': 'pty', 'timeout_seconds': 30,
                'input_limit': 1, 'output_limit': driver.MAX_STREAM_BYTES,
                'actions': [{'kind': 'write', 'data': b'\x03',
                    'after': {'stream': 'tty', 'marker': fixture.encode(records[0])+b'\r\n'}}]}
        value.run_case('sigint', ['tty', TOKEN], 130, plan=plan)
        argv = value.step.call_args_list[0].args[1]
        self.assertEqual(argv[-5:], ['--interactive', '--tty', IMAGE, 'tty', TOKEN])
        self.assertEqual(value.step.call_args_list[0].kwargs['plan'], plan)
        self.assertTrue(value.containers['sigint']['tty'])
        value.record.acknowledge_negative.assert_called_once()

    def test_cleanup_never_runs_for_uncertain_work_or_live_follower(self):
        for kind in ('incomplete', 'step', 'uncertain', 'missing-follower', 'live-follower', 'follower-uncertain'):
            value = bare(); value.workload_complete = kind != 'incomplete'
            value.steps = [{'complete': kind != 'step'}]
            value.record.receipts = [{'effects_uncertain': kind == 'uncertain'}]
            value.follower = SimpleNamespace(follow_thread=SimpleNamespace(is_alive=lambda: kind == 'live-follower'),
                record=SimpleNamespace(receipts=[{'effects_uncertain': kind == 'follower-uncertain'}]))
            if kind == 'missing-follower': value.follower = None
            value.step = Mock(side_effect=AssertionError('cleanup dispatched'))
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                value.cleanup()
            value.step.assert_not_called()

    def test_cleanup_refuses_live_or_foreign_container_before_remove(self):
        for key, changed in [('Running', True), ('Pid', 100), ('Pid', False)]:
            value = bare(); value.workload_complete = True
            value.follower = SimpleNamespace(follow_thread=SimpleNamespace(is_alive=lambda: False),
                                            record=SimpleNamespace(receipts=[]))
            value.containers = {'service': {'name': TOKEN+'-service', 'cid': CID}}
            item = {'Id': CID, 'Name': '/'+TOKEN+'-service', 'Image': IMAGE,
                    'Config': {'Labels': {lane.LABEL: TOKEN}}, 'State': {'Running': False, 'Pid': 0}}
            item['State'][key] = changed
            value.step = Mock(return_value=SimpleNamespace(stdout=json.dumps([item]).encode(), stderr=b''))
            with self.subTest(key=key), self.assertRaises(ValueError):
                value.cleanup()
            self.assertEqual(value.step.call_count, 1)
            self.assertEqual(value.step.call_args.args[1], ['container', 'inspect', CID])

    def test_container_inventory_and_engine_time_bounds(self):
        self.assertEqual(lane.container_ids((CID+'\n'+'b'*64+'\n').encode()), [CID, 'b'*64])
        self.assertEqual(lane.container_ids(b''), [])
        for raw in (b'\n', b'abc\n', (CID+'\n'+CID+'\n').encode(), b'\xff',
                    ('\n'.join('%064x' % n for n in range(257))+'\n').encode()):
            with self.subTest(raw=raw[:20]), self.assertRaises((ValueError, UnicodeError)):
                lane.container_ids(raw)
        self.assertEqual(lane.engine_time(1000000001), '1.000000001')
        for value in (True, 0, -1, 1.0):
            with self.assertRaises(ValueError):
                lane.engine_time(value)

    def test_health_deadline_uses_original_receipts_even_for_late_success(self):
        for duration in (15*10**9, 15*10**9+1):
            value = bare(); value.record.count = 0
            value.containers = {'service': {'cid': CID}}
            statuses = iter(('starting', 'healthy', 'unhealthy'))
            def guard():
                status = next(statuses)
                value.record.count += 1
                value.record.receipts.append({'index': value.record.count,
                    'started_unix_ns': value.record.count * 30*10**9,
                    'elapsed_ns': duration})
                return {'State': {'Health': {'Status': status,
                    'Log': [{'ExitCode': 0 if status == 'healthy' else 1}]}}}
            value.service_guard, value.step = guard, Mock()
            with patch.object(lane.time, 'monotonic', return_value=0), \
                 patch.object(lane.time, 'sleep', side_effect=AssertionError('unneeded poll')), \
                 patch('linux_docker_container_state.health_transition', return_value={'bounded': True}) as semantic:
                if duration == 15*10**9:
                    self.assertEqual(value.health(), {'bounded': True})
                    semantic.assert_called_once()
                else:
                    with self.assertRaises(ValueError):
                        value.health()
                    semantic.assert_not_called()

    def test_health_deadline_includes_recorded_gap_between_polls(self):
        value = bare(); value.record.count = 0
        value.containers = {'service': {'cid': CID}}
        def guard():
            value.record.count += 1
            value.record.receipts.append({'index': value.record.count,
                'started_unix_ns': 10**9 + (value.record.count-1)*16*10**9, 'elapsed_ns': 1})
            return {'State': {'Health': {'Status': 'starting',
                'Log': [] if value.record.count == 1 else [{'ExitCode': 1}]}}}
        value.service_guard = guard
        with patch.object(lane.time, 'monotonic', return_value=0), patch.object(lane.time, 'sleep') as sleep, \
             patch('linux_docker_container_state.health_transition') as semantic:
            with self.assertRaises(ValueError):
                value.health()
            sleep.assert_called_once_with(.25)
            semantic.assert_not_called()
        self.assertEqual(value.record.count, 2)

    def test_health_record_interval_rejects_missing_reversed_and_typed_drift(self):
        value = bare(); value.record.count = 2
        original = [{'index': 1, 'started_unix_ns': 100, 'elapsed_ns': 20},
                    {'index': 2, 'started_unix_ns': 150, 'elapsed_ns': 10}]
        value.record.receipts = original
        self.assertEqual(value.health_phase_elapsed(1), 60)
        self.assertEqual(value.health_phase_elapsed(2), 10)
        for key, changed in [('index', True), ('index', 3), ('started_unix_ns', 119),
                             ('started_unix_ns', True), ('elapsed_ns', False), ('elapsed_ns', -1)]:
            value.record.receipts = [dict(row) for row in original]
            value.record.receipts[1][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError):
                value.health_phase_elapsed(1)
        value.record.receipts = original[:1]
        with self.assertRaises(ValueError):
            value.health_phase_elapsed(1)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='vz-inert-lifecycle-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        data = inputs(); (self.root/'inputs.json').write_text(json.dumps(data))
        self.live = SimpleNamespace(inputs=SimpleNamespace(raw=data, scope=data['scope']), output=self.root,
                                    fixture=fixture.FIXTURE, selected=fixture.FIXTURE, env={},
                                    record=SimpleNamespace(count=0), steps=[])
        self.recorder = driver.Recorder(self.root, {}, [])

    def record(self, args, *, exit=0, stdout=b'', stderr=b''):
        argv = ['docker', '--config', '/owned/config', '--context', 'machine-context', *args]
        with patch.object(driver, 'execute', return_value=SimpleNamespace(returncode=exit, stdout=stdout, stderr=stderr)):
            command = self.recorder.run(argv, executable='/owned/docker', mutation=commands.mutation_for(args))
        self.live.record.count = self.recorder.count
        return command

    def test_replay_step_has_no_dispatch_or_write_and_exact_source_arguments(self):
        self.record(['info'], stdout=b'public')
        stored = {'label': 'selected-info', 'command_index': 1, 'complete': True}
        (self.root/'step-0001.json').write_text(json.dumps(stored))
        selected = lane.ReplayLifecycle(self.live)
        with patch.object(driver.Driver, 'command', side_effect=AssertionError('dispatch')), \
             patch.object(lane.startup, 'document', side_effect=AssertionError('write')):
            result = selected.step('selected-info', ['info'])
        self.assertEqual(result.stdout, b'public')
        self.assertEqual(selected.steps, [stored])
        for label, args in [('forged-label', ['info']), ('selected-info', ['version'])]:
            selected = lane.ReplayLifecycle(self.live)
            with self.assertRaises(ValueError):
                selected.step(label, args)

    def test_replay_negative_ack_is_exact_and_does_not_rewrite_raw(self):
        args = ['exec', CID, 'true']; command = self.record(args, exit=37)
        self.recorder.acknowledge_negative(command, 'exact public semantics')
        before = (self.root/'command-00001.json').read_bytes()
        selected = lane.ReplayLifecycle(self.live)
        observed = selected.command(args, expected=37)
        self.assertTrue(selected.record.receipts[0]['effects_uncertain'])
        with self.assertRaises(ValueError):
            selected.record.acknowledge_negative(observed, 'forged semantics')
        selected.record.acknowledge_negative(observed, 'exact public semantics')
        self.assertFalse(selected.record.receipts[0]['effects_uncertain'])
        self.assertEqual((self.root/'command-00001.json').read_bytes(), before)

    def test_replay_missing_command_and_wrong_input_refused(self):
        selected = lane.ReplayLifecycle(self.live)
        with self.assertRaises(ValueError):
            selected.command(['info'])
        data = inputs(); data['scope']['engine_id'] = 'foreign'
        (self.root/'inputs.json').write_text(json.dumps(data))
        with self.assertRaises(ValueError):
            lane.ReplayLifecycle(self.live)

    def test_interactive_replay_receives_external_plan_not_recorded_choice(self):
        args = ['attach', CID]; self.record(args, exit=37)
        selected = lane.ReplayLifecycle(self.live)
        plan = lane.io_plan([{'kind': 'write', 'data': b'public'}, {'kind': 'close_stdin'}])
        # A delegation test only: synthetic ordinary receipts are not passed
        # off as valid interactive evidence; the dedicated validator is mocked.
        with patch.object(lane.interactive, 'validate_recorded', return_value={}) as check, \
             patch.object(driver.Driver, 'command', side_effect=AssertionError('dispatch')):
            selected.command(args, expected=37, timeout=30, interaction_plan=plan)
        self.assertEqual(check.call_args.kwargs['expected_plan'], plan)
        self.assertEqual(check.call_args.kwargs['argv'][-2:], ['attach', CID])
        self.assertEqual(check.call_args.kwargs['expected_exit'], 37)

    def test_final_replay_rejects_extra_or_uncertain_commands(self):
        workload = {'inert_test_program': True}
        (self.root/'workload.json').write_text(json.dumps(workload))
        def program(selected):
            selected.record.count = 1
            selected.record.receipts = [{'effects_uncertain': False}]
            return workload
        self.live.record.count = 1
        (self.root/'command-00001.json').write_text('{}')
        with patch.object(lane.ReplayLifecycle, 'exercise', program), \
             patch.object(driver.Driver, 'command', side_effect=AssertionError('dispatch')):
            lane.replay(self.live, cleanup=False)
            (self.root/'command-00002.json').write_text('{}')
            with self.assertRaises(ValueError):
                lane.replay(self.live, cleanup=False)
            (self.root/'command-00002.json').unlink()
        def uncertain(selected):
            program(selected); selected.record.receipts[0]['effects_uncertain'] = True
            return workload
        with patch.object(lane.ReplayLifecycle, 'exercise', uncertain), self.assertRaises(ValueError):
            lane.replay(self.live, cleanup=False)


if __name__ == '__main__':
    unittest.main()
