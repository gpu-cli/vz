"""Inert adapter/replay fixtures only: no tmux, Docker or guest is launched."""
import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import linux_docker_container_tmux as m

CID = 'a' * 64
TOKEN = 'vzio-' + 'b' * 24
ENV = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'}


class Adapter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='vz-tmux-adapter-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.tool = self.root / 'tool'
        self.tool.write_bytes(b'inert tool, never dispatched')
        self.tool.chmod(0o700)
        self.inputs = {'docker_config': str(self.root / 'docker'),
                       'scope': {'docker_context': 'vzr1-docker_context-docker-' + 'c'*30},
                       'clients': {'docker': {'path': str(self.tool)}}}

    def test_fixed_multicall_invocation_and_no_ambient_environment(self):
        row = m.binding(self.inputs, CID, TOKEN, ENV)
        self.assertEqual(row['argv'], ['docker', '--config', self.inputs['docker_config'], '--context',
            self.inputs['scope']['docker_context'], 'exec', '--interactive', '--tty', '--user', '0:0',
            '--workdir', '/workspace', CID, 'python3', '-u', '/fixture/probe.py', 'tty', TOKEN])
        self.assertEqual(row['environment'], ENV)
        self.assertIsNot(row['environment'], ENV)
        self.assertIn(b'os.execve(', m.LAUNCHER)
        self.assertNotIn(b'shell', m.LAUNCHER)

    def test_foreign_container_context_environment_rejected(self):
        for kind in ('name', 'short', 'context', 'relative', 'ambient', 'ssh', 'tmux'):
            inputs = copy.deepcopy(self.inputs); cid = CID; env = ENV.copy()
            if kind == 'name': cid = 'owned-name'
            if kind == 'short': cid = CID[:12]
            if kind == 'context': inputs['scope']['docker_context'] = 'default'
            if kind == 'relative': inputs['docker_config'] = 'docker'
            if kind == 'ambient': env['DOCKER_HOST'] = 'unix:///default'
            if kind == 'ssh': env['SSH_AUTH_SOCK'] = '/private/socket'
            if kind == 'tmux': env['TMUX'] = '/default'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                m.binding(inputs, cid, TOKEN, env)

    def test_launcher_execve_argv0_and_argument_guard(self):
        launch = self.root / 'launcher.py'
        launch.write_bytes(m.LAUNCHER)
        m.core.document(self.root / 'binding.json', m.binding(self.inputs, CID, TOKEN, ENV))
        with patch.object(sys, 'argv', [str(launch), 'tty', TOKEN]), patch('os.execve') as execute:
            exec(compile(m.LAUNCHER, str(launch), 'exec'), {'__file__': str(launch)})
        execute.assert_called_once_with(str(self.tool), m.binding(self.inputs, CID, TOKEN, ENV)['argv'], ENV)
        with patch.object(sys, 'argv', [str(launch), 'tty', 'foreign']), patch('os.execve') as execute:
            with self.assertRaises(SystemExit):
                exec(compile(m.LAUNCHER, str(launch), 'exec'), {'__file__': str(launch)})
            execute.assert_not_called()

    def test_registration_precedes_launch_and_guards_surround_work(self):
        events = []
        item = SimpleNamespace(output=self.root, inputs=SimpleNamespace(raw=self.inputs), env=ENV,
                               guard=lambda: events.append('machine'))
        owner = SimpleNamespace(run=lambda: events.append('run') or {'passed': True}, server=None, pending=[])
        def registered(actual):
            self.assertIs(actual, owner); events.append('registered')
        with patch.object(m.fixture, 'fixture_contract'), patch.object(m, 'DockerSmoke', return_value=owner), \
             patch.object(m, 'replay_tmux', return_value={'scoped': True}) as replay:
            result = m.run_tmux(item, CID, TOKEN, tmux_path=self.tool,
                service_guard=lambda *args: events.append('service'), register_owner=registered)
        self.assertEqual(events, ['machine', 'service', 'registered', 'run', 'machine', 'service'])
        self.assertEqual(result, {'scoped': True})
        self.assertEqual(replay.call_args.kwargs, {'environment': ENV, 'tmux_path': self.tool})

    def test_failure_retains_registered_owner_without_acceptance(self):
        retained = []
        item = SimpleNamespace(output=self.root, inputs=SimpleNamespace(raw=self.inputs), env=ENV, guard=lambda: None)
        owner = SimpleNamespace(run=lambda: {'passed': False}, server=object(), pending=[object()])
        with patch.object(m.fixture, 'fixture_contract'), patch.object(m, 'DockerSmoke', return_value=owner), \
             patch.object(m, 'replay_tmux') as replay:
            with self.assertRaises(ValueError):
                m.run_tmux(item, CID, TOKEN, tmux_path=self.tool, service_guard=lambda *args: None,
                           register_owner=retained.append)
        self.assertEqual(retained, [owner]); replay.assert_not_called()
        self.assertFalse((self.root / 'tmux/proof.json').exists())

    def fixture(self):
        root = self.root / 'tmux'; root.mkdir()
        directory = root / 'smoke'; directory.mkdir()
        (root / 'launcher.py').write_bytes(m.LAUNCHER)
        self.write(root / 'binding.json', m.binding(self.inputs, CID, TOKEN, ENV))
        private = '/private/tmp/vzio-tmux-inert123'
        socket, session, pane = private + '/server.sock', 'vzio-' + 'd'*32, '%0'
        python = str(Path(sys.executable).resolve())
        paths = (self.tool, Path(python), root / 'launcher.py', root / 'binding.json',
                 Path(m.__file__).resolve(), Path(m.core.__file__).resolve(), Path(m.core.capture.__file__).resolve())
        self.write(directory / 'inputs.json', {'schema_version': 1,
            'inputs_sha256': {str(p): m.core.sha(p.read_bytes()) for p in paths},
            'fixture': str(root / 'launcher.py'), 'token': TOKEN, 'session_name': session,
            'private_root': private, 'socket': socket, 'environment': m.core.ENV,
            'scope': 'owned_docker_exec_tmux_adapter_not_local_probe'})
        original = {'server_pid': 500, 'session_id': '$0', 'pane_id': pane, 'pane_pid': 501,
                    'tty': '/dev/ttys001', 'cols': 80, 'rows': 24}
        records = [dict(schema_version=1, token=TOKEN, type='tty_ready', cols=80, rows=24, isatty=[True]*3),
                   dict(schema_version=1, token=TOKEN, type='tty_size', cols=120, rows=40),
                   dict(schema_version=1, token=TOKEN, type='tty_done', exit_code=37)]
        self.write(directory / 'result.json', {'schema_version': 1, 'passed': True, 'error': None,
            'cleanup_error': None, 'cleanup_fallback': None, 'server_reaped': True, 'socket_removed': True,
            'pane_dead_status': 37, 'host_termios_restoration_certified': False,
            'original_identity': original, 'resized_identity': original | {'cols': 120, 'rows': 40}, 'records': records})
        rows = [
            ('status-off', ['set-option', '-g', 'status', 'off'], b''),
            ('remain', ['set-window-option', '-g', 'remain-on-exit', 'on'], b''),
            ('remain-format', ['set-window-option', '-g', 'remain-on-exit-format', ''], b''),
            ('create', ['new-session', '-d', '-s', session, '-x', '80', '-y', '24', python,
                        '-B', '-u', str(root / 'launcher.py'), 'tty', TOKEN], b''),
            ('identity', ['display-message', '-p', '-t', session,
                '#{pid}|#{session_id}|#{pane_id}|#{pane_pid}|#{pane_tty}|#{pane_width}|#{pane_height}'],
                b'500|$0|%0|501|/dev/ttys001|80|24\n'),
            ('manual-size', ['set-window-option', '-t', pane, 'window-size', 'manual'], b''),
            ('frame-ready', ['capture-pane', '-p', '-J', '-S', '-', '-t', pane], m.evidence.canonical(records[0])),
            ('ansi-ready', ['capture-pane', '-p', '-e', '-S', '-', '-t', pane], b'raw visible frame'),
            ('resize', ['resize-window', '-t', pane, '-x', '120', '-y', '40'], b''),
            ('identity', ['display-message', '-p', '-t', session,
                '#{pid}|#{session_id}|#{pane_id}|#{pane_pid}|#{pane_tty}|#{pane_width}|#{pane_height}'],
                b'500|$0|%0|501|/dev/ttys001|120|40\n'),
            ('send-size', ['send-keys', '-t', pane, '-l', 'size'], b''),
            ('size-enter', ['send-keys', '-t', pane, 'Enter'], b''),
            ('frame-size', ['capture-pane', '-p', '-J', '-S', '-', '-t', pane], b''.join(map(m.evidence.canonical, records[:2]))),
            ('ansi-size', ['capture-pane', '-p', '-e', '-S', '-', '-t', pane], b'raw visible frame'),
            ('send-exit', ['send-keys', '-t', pane, '-l', 'exit'], b''),
            ('exit-enter', ['send-keys', '-t', pane, 'Enter'], b''),
            ('frame-done', ['capture-pane', '-p', '-J', '-S', '-', '-t', pane], b''.join(map(m.evidence.canonical, records))),
            ('ansi-done', ['capture-pane', '-p', '-e', '-S', '-', '-t', pane], b'raw visible frame'),
            ('pane-exit', ['display-message', '-p', '-t', pane, '#{pane_dead}|#{pane_dead_status}'], b'1|37\n'),
            ('cleanup-server-identity', ['display-message', '-p', '#{pid}'], b'500\n'),
            ('kill-owned-server', ['kill-server'], b'')]
        for i, (label, args, raw) in enumerate(rows, 1):
            stem = '%03d-%s' % (i, label)
            argv = [str(self.tool), '-N', '-f', '/dev/null', '-S', socket, *args]
            clock = lambda offset: {'unix_ns': 10000 + i*100 + offset, 'monotonic_ns': 20000 + i*100 + offset}
            self.write(directory / (stem + '.intent.json'), {'argv': argv, 'executable': str(self.tool),
                       'environment': m.core.ENV, 'started_unix_ns': clock(0)['unix_ns'], 'socket_identity': [1,2,501]})
            receipt = {'schema_version': 1, 'argv': argv, 'executable': str(self.tool), 'cwd': private,
                'environment': m.core.ENV, 'capture_complete': True, 'effects_uncertain': False,
                'owned_process_reaped': True, 'owned_direct_child': True, 'returncode': 0,
                'error': None, 'cleanup_error': None, 'termination': None,
                'pid': 600+i, 'process_group': 600+i, 'session_id': 600+i, 'mode': 'pipes',
                'timeout_seconds': 5, 'input_limit': 1, 'output_limit_each': m.core.LIMIT,
                'planned_action_count': 1, 'merged_tty': False,
                'outputs': {k: {'size': len(v), 'sha256': m.core.sha(v)}
                            for k,v in {'stdout': raw, 'stderr': b'', 'tty': b''}.items()},
                'started': clock(1), 'completed': clock(9), 'stdin_eof_count': 1, 'terminal': None,
                'actions': [{'index': 0, 'kind': 'close_stdin', 'complete': True, 'triggered': clock(2),
                             'completed': clock(3), 'observed_bytes': {'stdout':0,'stderr':0,'tty':0},
                             'trigger': {'kind': 'immediate'}}]}
            self.write(directory / (stem + '.result.json'), receipt)
            (directory / (stem + '.stdout')).write_bytes(raw)
            (directory / (stem + '.stderr')).write_bytes(b'')
        self.write(directory / 'server.intent.json', {'argv': [str(self.tool), '-D', '-f', '/dev/null', '-S', socket],
            'environment': m.core.ENV, 'foreground_owned_child': True, 'started_unix_ns': 100})
        self.write(directory / 'server.started.json', {'pid': 500, 'owned_direct_child': True, 'started_unix_ns': 200})
        self.write(directory / 'server.disposition.json', {'owned_pid': 500, 'returncode': 0, 'reaped': True,
            'socket_removed': True, 'cleanup_error': None, 'fallback': None, 'finished_unix_ns': 20000})
        self.write(directory / 'socket-retirement.intent.json', {'server_pid': 500, 'server_returncode': 0,
            'server_reaped': True, 'private_root': private, 'socket': socket,
            'socket_identity': [1,2,501], 'started_unix_ns': 15000})
        self.write(directory / 'socket-retirement.result.json', {'socket_removed': True,
            'private_directory_removed': True, 'finished_unix_ns': 16000})
        for stream in ('stdout','stderr'): (directory / ('server.' + stream)).write_bytes(b'')
        self.seal(root)
        return root

    def write(self, path, row):
        path.write_bytes(m.evidence.canonical(row))

    def seal(self, root):
        d = root / 'smoke'
        (d / 'checksums.sha256').write_text(''.join(m.core.sha(p.read_bytes()) + '  ' + p.name + '\n'
            for p in sorted(d.iterdir()) if p.name != 'checksums.sha256'))

    def replay(self, root):
        return m.replay_tmux(root, self.inputs, CID, TOKEN, environment=ENV, tmux_path=self.tool)

    def test_complete_synthetic_ledger_replays_but_makes_no_physical_claim(self):
        proof = self.replay(self.fixture())
        self.assertEqual(proof['command_count'], 21)
        self.assertEqual(proof['pane_dead_status'], 37)
        self.assertFalse(proof['host_termios_restoration_certified'])
        self.assertIn('external_service_guard', proof['scope'])

    def test_resealed_command_clock_frame_exit_and_cleanup_mutations_fail(self):
        root = self.fixture()
        mutations = [
            ('004-create.intent.json', lambda x: x['argv'].__setitem__(-1, 'foreign')),
            ('009-resize.intent.json', lambda x: x.update(started_unix_ns=1)),
            ('009-resize.result.json', lambda x: x.update(owned_process_reaped=False)),
            ('result.json', lambda x: x.update(pane_dead_status=0)),
            ('result.json', lambda x: x.update(host_termios_restoration_certified=True)),
            ('result.json', lambda x: x.update(cleanup_fallback={'reaped': True})),
            ('server.disposition.json', lambda x: x.update(returncode=-15)),
            ('socket-retirement.result.json', lambda x: x.update(private_directory_removed=False))]
        for name, mutate in mutations:
            path = root / 'smoke' / name; original = path.read_bytes(); row = json.loads(original)
            mutate(row); self.write(path, row); self.seal(root)
            with self.subTest(name=name), self.assertRaises(ValueError): self.replay(root)
            path.write_bytes(original); self.seal(root)
        path = root / 'smoke/017-frame-done.stdout'; original = path.read_bytes()
        path.write_bytes(original.replace(b'tty_done', b'input_echo')); self.seal(root)
        with self.assertRaises(ValueError): self.replay(root)

    def test_manifest_and_launcher_mutations_fail(self):
        root = self.fixture()
        path = root / 'launcher.py'; path.write_bytes(m.LAUNCHER + b'# changed\n')
        with self.assertRaises(ValueError): self.replay(root)
        path.write_bytes(m.LAUNCHER)
        (root / 'smoke/extra').write_bytes(b'foreign')
        with self.assertRaises(ValueError): self.replay(root)


if __name__ == '__main__':
    unittest.main()
