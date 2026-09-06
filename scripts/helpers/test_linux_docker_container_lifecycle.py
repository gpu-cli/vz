"""Inert source-program/replay checks; dispatch and process creation prohibited."""
import json
import importlib.util
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
    value.killer = value.kill_termination = None
    value.terminal_owner = None
    value.tmux_proof = None
    return value


class SourcePlanTests(unittest.TestCase):
    def failed_run(self, code, *, stderr=None, returned=None):
        value = bare(); value.absent = Mock()
        role, entrypoint = ('nonexec', '/fixture/not-executable') if code == 126 else ('missing', '/fixture/does-not-exist')
        name = TOKEN+'-'+role
        state = {'Id': CID, 'Name': '/'+name, 'Image': IMAGE,
            'Config': {'Labels': {lane.LABEL: TOKEN}, 'Entrypoint': [entrypoint], 'Cmd': None},
            'HostConfig': {'Runtime': 'youki', 'NetworkMode': 'none'},
            'State': {'Running': False, 'Pid': 0}, 'Mounts': []}
        result = SimpleNamespace(index=1, returncode=code if returned is None else returned, stdout=b'',
                                 stderr=lane.failed_start_diagnostic(code, entrypoint) if stderr is None else stderr)
        value.step = Mock(side_effect=[result, SimpleNamespace(stdout=json.dumps([state]).encode(), stderr=b'')])
        return value, role, entrypoint

    def test_failed126_and127_exact_diagnostics_are_acknowledged_after_ownership(self):
        for code in (126,127):
            entrypoint = '/fixture/not-executable' if code == 126 else '/fixture/does-not-exist'
            value, role, entrypoint = self.failed_run(code, stderr=b'docker: runtime create failed: ' +
                lane.failed_start_diagnostic(code, entrypoint) + b'\n\nDocker help trailer\n')
            proof = value.run_case(role, [], code, entrypoint=entrypoint)
            self.assertEqual(proof['exit'], code); self.assertEqual(proof['cid'], CID)
            self.assertEqual(value.step.call_args_list[0].kwargs['expected'], code)
            value.record.acknowledge_negative.assert_called_once()
        self.assertIn(b"permission denied: executable '/fixture/not-executable' at path '\"/fixture/not-executable\"'",
                      lane.failed_start_diagnostic(126, '/fixture/not-executable'))
        self.assertIn(b"executable file not found: executable '/fixture/does-not-exist' not found in $PATH",
                      lane.failed_start_diagnostic(127, '/fixture/does-not-exist'))

    def test_failed126_and127_wrong_diagnostic_or_code_never_acknowledged(self):
        for code, entrypoint in ((126,'/fixture/not-executable'),(127,'/fixture/does-not-exist')):
            exact = lane.failed_start_diagnostic(code, entrypoint)
            prefix = b'permission denied: ' if code==126 else b'executable file not found: '
            for kind in ('old', 'path', 'case', 'code', 'duplicate'):
                stderr = exact
                if kind=='old': stderr=exact.replace(prefix,b'')
                if kind=='path': stderr=exact.replace(entrypoint.encode(),b'/fixture/foreign')
                if kind=='case': stderr=exact.replace(prefix,prefix[:1].upper()+prefix[1:])
                if kind=='duplicate': stderr=exact+b'\n'+exact
                value, role, _ = self.failed_run(code, stderr=stderr, returned=125 if kind=='code' else code)
                with self.subTest(code=code,kind=kind),self.assertRaises(ValueError):
                    value.run_case(role, [], code, entrypoint=entrypoint)
                value.record.acknowledge_negative.assert_not_called()

    def test_tty_failure_observer_preserves_original_predicate_and_exit(self):
        import os
        import sys
        for failing in (False, True):
            observed = []
            def original(value):
                if not value:
                    raise ValueError('unchanged probe rejection')
            probe = SimpleNamespace(token=lambda value: value, require=original,
                                    emit=lambda *args, **kwargs: observed.append((args, kwargs)))
            def size():
                rows, columns = (0, 0) if failing else (24, 80)
                probe.require(rows > 0 and columns > 0)
            def main(args):
                self.assertEqual(args, ['tty', TOKEN])
                try:
                    size()
                    return 130
                except ValueError:
                    return 70
            probe.main = main
            spec = SimpleNamespace(loader=SimpleNamespace(exec_module=Mock()))
            with patch.object(importlib.util, 'spec_from_file_location', return_value=spec) as load, \
                 patch.object(importlib.util, 'module_from_spec', return_value=probe), \
                 patch.object(sys, 'argv', ['-c', TOKEN]), patch.object(os, 'isatty', return_value=True), \
                 self.assertRaises(SystemExit) as terminal:
                exec(compile(lane.TTY_START, '<frozen-tty-observer>', 'exec'), {})
            self.assertEqual(terminal.exception.code, 70 if failing else 130)
            load.assert_called_once_with('vzio_probe', '/fixture/probe.py')
            spec.loader.exec_module.assert_called_once_with(probe)
            if failing:
                self.assertEqual(len(observed), 1)
                args, details = observed[0]
                self.assertEqual(args, ('tty_contract_failure', TOKEN))
                self.assertEqual(details['check'], 'size')
                self.assertEqual((details['rows'], details['cols']), (0, 0))
                self.assertEqual(details['isatty'], [True, True, True])
                self.assertIsNone(details['lflag'])
                self.assertIs(details['stream'], sys.stderr.buffer)
            else:
                self.assertEqual(observed, [])

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
            value.killer = value.follower
            value.terminal_owner = SimpleNamespace(pending=[], server=SimpleNamespace(returncode=0))
            value.tmux_proof = {'inert_test': True}
            value.containers = {'service': {'name': TOKEN+'-service', 'cid': CID}}
            item = {'Id': CID, 'Name': '/'+TOKEN+'-service', 'Image': IMAGE,
                    'Config': {'Labels': {lane.LABEL: TOKEN}}, 'State': {'Running': False, 'Pid': 0}}
            item['State'][key] = changed
            value.step = Mock(return_value=SimpleNamespace(stdout=json.dumps([item]).encode(), stderr=b''))
            with self.subTest(key=key), self.assertRaises(ValueError):
                value.cleanup()
            self.assertEqual(value.step.call_count, 1)
            self.assertEqual(value.step.call_args.args[1], ['container', 'inspect', CID])

    def test_cleanup_refuses_missing_or_unresolved_terminal_before_dispatch(self):
        for kind in ('missing', 'pending', 'live', 'signal', 'bool', 'proof'):
            value = bare(); value.workload_complete = True
            value.follower = SimpleNamespace(follow_thread=SimpleNamespace(is_alive=lambda: False),
                                            record=SimpleNamespace(receipts=[]))
            value.killer = value.follower
            value.terminal_owner = SimpleNamespace(pending=[], server=SimpleNamespace(returncode=0))
            value.tmux_proof = {'inert_test': True}
            if kind == 'missing': value.terminal_owner = None
            if kind == 'pending': value.terminal_owner.pending = [object()]
            if kind in ('live', 'signal', 'bool'):
                value.terminal_owner.server.returncode = {'live': None, 'signal': -15, 'bool': False}[kind]
            if kind == 'proof': value.tmux_proof = None
            value.step = Mock(side_effect=AssertionError('cleanup dispatched'))
            with self.subTest(kind=kind), self.assertRaises(ValueError): value.cleanup()
            value.step.assert_not_called()

    def test_sigkill_uses_normal_service_and_registers_before_dispatch(self):
        value = bare(); value.absent = Mock()
        def run(row):
            self.assertIs(value.containers['sigkill'], row)
            self.assertIsNone(row['cid'])
            self.assertEqual(row['command'], ['service', TOKEN])
            self.assertEqual(row['entrypoint'], lane.ENTRYPOINT)
            self.assertFalse(row['interactive'])
            return {'inert_test': True}
        value.run_kill = Mock(side_effect=run)
        self.assertEqual(value.sigkill(), {'inert_test': True})
        value.absent.assert_called_once_with(TOKEN+'-sigkill')
        with self.assertRaises(ValueError): value.sigkill()

    def test_external_sigkill_uses_exact_cid_and_source_kill_timestamp(self):
        value = bare(); value.record.count = 0
        value.containers = {'sigkill': {'name': TOKEN+'-sigkill', 'cid': None}}
        initial = {'State': {'StartedAt': 'same-generation'}, 'RestartCount': 0}
        final = {'State': {'StartedAt': 'same-generation'}, 'RestartCount': 0, 'inert_exit': 137}
        value.inspect = Mock(side_effect=[initial, initial, final])
        calls = []
        def add():
            value.record.count += 1
            value.record.receipts.append({'started_unix_ns': value.record.count*100})
        def guard(): add(); add()
        def step(label, args):
            add(); calls.append((label, args, value.record.count))
            raw = json.dumps([{'Id': CID}]).encode() if label == 'resolve-live-sigkill' else (
                (CID+'\n').encode() if label == 'signal-run-kill' else b'137\n')
            return SimpleNamespace(index=value.record.count, stdout=raw, stderr=b'')
        value.guard, value.step = guard, step
        with patch('linux_docker_container_state.same_generation') as generation, \
             patch('linux_docker_container_state.same_identity') as identity, \
             patch('linux_docker_container_state.stopped') as stopped:
            result = value.terminate_kill()
        self.assertEqual(calls, [('resolve-live-sigkill', ['container', 'inspect', TOKEN+'-sigkill'], 3),
            ('signal-run-kill', ['container', 'kill', '--signal', 'KILL', CID], 6),
            ('wait-run-kill137', ['container', 'wait', CID], 7)])
        self.assertEqual(result, {'cid': CID, 'command_index': 6, 'started_unix_ns': 600})
        generation.assert_called_once_with(initial, initial)
        identity.assert_called_once_with(initial, final)
        stopped.assert_called_once_with(final, 137)
        with self.assertRaises(ValueError): value.terminate_kill()
        for changed in (final | {'State': {'StartedAt': 'later-generation'}}, final | {'RestartCount': 1}):
            value.kill_termination = None; value.containers['sigkill']['cid'] = None
            value.inspect = Mock(side_effect=[initial, initial, changed])
            with patch('linux_docker_container_state.same_generation'), \
                 patch('linux_docker_container_state.same_identity'), \
                 patch('linux_docker_container_state.stopped') as stopped, self.assertRaises(ValueError):
                value.terminate_kill()
            stopped.assert_not_called()
        value.kill_termination = None; value.containers['sigkill']['cid'] = None
        value.inspect = Mock(side_effect=[initial, initial]); calls.clear()
        with patch('linux_docker_container_state.same_generation', side_effect=ValueError('generation drift')), \
             self.assertRaises(ValueError): value.terminate_kill()
        self.assertEqual([row[0] for row in calls], ['resolve-live-sigkill'])

    def test_cleanup_refuses_unresolved_kill_run_observer(self):
        for kind in ('missing', 'live', 'uncertain'):
            value = bare(); value.workload_complete = True
            value.follower = SimpleNamespace(follow_thread=SimpleNamespace(is_alive=lambda: False),
                                            record=SimpleNamespace(receipts=[]))
            value.killer = SimpleNamespace(follow_thread=SimpleNamespace(is_alive=lambda: kind == 'live'),
                record=SimpleNamespace(receipts=[{'effects_uncertain': kind == 'uncertain'}]))
            if kind == 'missing': value.killer = None
            value.step = Mock(side_effect=AssertionError('cleanup dispatched'))
            with self.subTest(kind=kind), self.assertRaises(ValueError): value.cleanup()
            value.step.assert_not_called()

    def test_tmux_external_window_rejects_self_selected_clock_and_missing_guards(self):
        value = bare(); value.record.count = 10
        rows = [{'index': i, 'started_unix_ns': i*100, 'elapsed_ns': 10} for i in range(1, 11)]
        value.record.receipts = rows
        proof = {'started_unix_ns': 510, 'finished_unix_ns': 600}
        self.assertEqual(value.tmux_window(1, proof), proof | {'guard_first_command': 1, 'guard_last_command': 10})
        for key, changed in [('started_unix_ns', 509), ('finished_unix_ns', 601),
                             ('started_unix_ns', 601), ('started_unix_ns', True)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                value.tmux_window(1, proof | {key: changed})
        for key, changed in [('index', True), ('started_unix_ns', 499), ('elapsed_ns', -1)]:
            value.record.receipts = [dict(row) for row in rows]
            value.record.receipts[5][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): value.tmux_window(1, proof)
        value.record.receipts = rows[:-1]
        with self.assertRaises(ValueError): value.tmux_window(1, proof)

    def test_tmux_owner_retained_before_adapter_failure(self):
        value = bare(); value.record.count = 0; value._tmux_path = '/owned/tmux'
        value.verify_terminal_pins = Mock(); value.containers = {'service': {'cid': CID}}
        owner = SimpleNamespace(pending=[object()], server=object())
        def run(item, cid, token, **kwargs):
            self.assertIs(item, value); self.assertEqual((cid, token), (CID, TOKEN))
            self.assertEqual(kwargs['tmux_path'], '/owned/tmux')
            kwargs['register_owner'](owner)
            raise ValueError('owned terminal failed')
        with patch('linux_docker_container_tmux.run_tmux', side_effect=run), self.assertRaises(ValueError):
            value.tmux()
        self.assertIs(value.terminal_owner, owner)
        self.assertIsNone(value.tmux_proof)

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
                                    _tmux_path='/owned/tmux', _terminal_pins={},
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

    def test_terminal_replay_consumes_guards_without_launch_or_write(self):
        selected = lane.ReplayLifecycle(self.live)
        selected.containers = {'service': {'cid': CID}}
        selected.verify_terminal_pins = Mock()
        events = []
        def guard_rows(count, label):
            events.append(label)
            for _ in range(count):
                selected.record.count += 1
                i = selected.record.count
                selected.record.receipts.append({'index': i, 'started_unix_ns': i*100, 'elapsed_ns': 10})
        selected.guard = lambda: guard_rows(2, 'machine')
        selected.service_guard = lambda cid, token: guard_rows(3, 'service')
        proof = {'started_unix_ns': 510, 'finished_unix_ns': 600}
        (self.root/'tmux').mkdir(); (self.root/'tmux/proof.json').write_text(json.dumps(proof))
        def raw(*args, **kwargs):
            events.append('replay'); self.assertEqual(args, (self.root/'tmux', self.live.inputs.raw, CID, selected.token))
            self.assertEqual(kwargs, {'environment': {}, 'tmux_path': '/owned/tmux'})
            return proof
        with patch('linux_docker_container_tmux.replay_tmux', side_effect=raw), \
             patch('linux_docker_container_tmux.run_tmux', side_effect=AssertionError('launch')), \
             patch.object(lane.startup, 'document', side_effect=AssertionError('write')):
            result = selected.tmux()
        self.assertEqual(events, ['machine', 'service', 'replay', 'machine', 'service'])
        self.assertEqual(result['guard_last_command'], 10)
        self.assertEqual(selected.terminal_owner.server.returncode, 0)
        self.assertEqual(selected.verify_terminal_pins.call_count, 2)

    def test_kill_run_replay_uses_retained_filename_and_external_termination(self):
        selected = lane.ReplayLifecycle(self.live); selected.image_id = IMAGE
        output = self.root/'kill-run'; output.mkdir()
        proof = {'inert_delegation_test': True}
        (output/'kill-proof.json').write_text(json.dumps(proof))
        for i in range(1, 6):
            (output/('command-%05d.json' % i)).write_text(json.dumps({'effects_uncertain': i == 3}))
        events = []; selected.guard = lambda: events.append('guard')
        termination = {'cid': CID, 'command_index': 8, 'started_unix_ns': 800}
        selected.terminate_kill = lambda: events.append('kill') or termination
        row = {'name': selected.token+'-sigkill'}
        with patch('linux_docker_container_kill.replay_kill', return_value=proof) as replay, \
             patch('linux_docker_container_kill.run_kill', side_effect=AssertionError('launch')), \
             patch.object(driver.Driver, 'command', side_effect=AssertionError('dispatch')), \
             patch.object(lane.startup, 'document', side_effect=AssertionError('write')):
            self.assertEqual(selected.run_kill(row), proof)
        self.assertEqual(events, ['guard', 'guard', 'kill', 'guard'])
        self.assertEqual(replay.call_args.args, (output, self.live.inputs.raw, row['name'], IMAGE, selected.token, proof))
        self.assertEqual(replay.call_args.kwargs, {'environment': {'TMPDIR': str(output/'private-tmp')},
                                                 'termination': termination})
        self.assertFalse(selected.killer.follow_thread.is_alive())
        self.assertFalse(any(row['effects_uncertain'] for row in selected.killer.record.receipts))

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
