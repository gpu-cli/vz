"""Only bounded local Python children; no Docker, VM, SSH or user terminal."""
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import linux_docker_interactive_capture as io


def plan(mode='pipes', actions=None, timeout=3, limit=io.MAX_BYTES):
    return {'schema_version': 1, 'mode': mode, 'timeout_seconds': timeout,
            'input_limit': io.MAX_BYTES, 'output_limit': limit, 'actions': actions or []}


def after(stream, marker):
    return {'stream': stream, 'marker': marker}


class Interactive(unittest.TestCase):
    def run_child(self, script, selected, env=None):
        with tempfile.TemporaryDirectory(prefix='vz-interactive-unit-') as root:
            result = io.capture([sys.executable, '-c', script], executable=sys.executable,
                                cwd=root, env=env or {'PATH': '/usr/bin:/bin'}, plan=selected)
        self.assertIsNone(result.pending_process)
        self.assertTrue(result.receipt['owned_process_reaped'])
        json.dumps(result.receipt)
        return result

    def test_binary_stdin_half_close_delayed_two_streams_and_exit37(self):
        data = bytes(range(256)) * 1025
        selected = plan(actions=[{'kind': 'write', 'data': data}, {'kind': 'close_stdin'}])
        result = self.run_child('import sys,time; b=sys.stdin.buffer.read(); '
            'sys.stdout.buffer.write(b);sys.stdout.buffer.flush();time.sleep(.02);'
            'sys.stdout.buffer.write(b"trailing");sys.stderr.buffer.write(b"error");sys.exit(37)', selected)
        self.assertEqual(result.stdout, data + b'trailing')
        self.assertEqual(result.stderr, b'error')
        self.assertEqual(result.returncode, 37)
        self.assertTrue(result.receipt['capture_complete'])
        self.assertFalse(result.receipt['effects_uncertain'])
        self.assertEqual(result.receipt['stdin_eof_count'], 1)
        self.assertEqual(result.receipt['actions'][0]['written_bytes'], len(data))
        self.assertNotIn('data', result.receipt['actions'][0])

    def test_split_stdout_marker_orders_input_and_records_offset(self):
        selected = plan(actions=[{'kind': 'write', 'data': b'yes\n', 'after': after('stdout', b'READY\n')},
                                 {'kind': 'close_stdin'}])
        result = self.run_child('import sys,time;sys.stdout.write("RE");sys.stdout.flush();'
            'time.sleep(.03);sys.stdout.write("ADY\\n");sys.stdout.flush();'
            'sys.stdout.buffer.write(sys.stdin.buffer.read())', selected)
        self.assertEqual(result.stdout, b'READY\nyes\n')
        trigger = result.receipt['actions'][0]['trigger']
        self.assertEqual((trigger['start_offset'], trigger['end_offset']), (0, 6))
        self.assertEqual(trigger['marker_sha256'], io.digest(b'READY\n'))

    def test_stderr_marker_and_owned_sigterm(self):
        result = self.run_child('import os,signal,time;'
            'signal.signal(signal.SIGTERM,lambda a,b:os._exit(37));'
            'os.write(2,b"READY");time.sleep(2)',
            plan(actions=[{'kind': 'signal', 'name': 'SIGTERM', 'after': after('stderr', b'READY')}]))
        self.assertEqual(result.returncode, 37)
        self.assertTrue(result.receipt['capture_complete'])
        self.assertEqual(result.receipt['actions'][0]['signal_scope'], 'owned_cli_group')

    def test_owned_sigint_exit_status(self):
        result = self.run_child('import os,signal,time;signal.signal(signal.SIGINT,signal.SIG_DFL);'
            'os.write(1,b"READY");time.sleep(2)',
            plan(actions=[{'kind': 'signal', 'name': 'SIGINT', 'after': after('stdout', b'READY')}]))
        self.assertEqual(result.returncode, -signal.SIGINT)

    def test_environment_exact_no_inherited_stdin(self):
        result = self.run_child('import os,sys;assert os.environ.get("ONLY")=="public";'
            'assert "SSH_AUTH_SOCK" not in os.environ;assert "HOME" not in os.environ;'
            'assert not sys.stdin.isatty();print("ok")', plan(), {'ONLY': 'public'})
        self.assertEqual(result.stdout, b'ok\n')
        self.assertEqual(result.receipt['environment'], {'ONLY': 'public'})

    def test_output_overflow_keeps_bounded_prefix_and_reaps(self):
        result = self.run_child('import os,time;os.write(1,b"a"*8192);time.sleep(2)', plan(limit=1024))
        self.assertEqual(result.stdout, b'a' * 1024)
        self.assertEqual(result.receipt['error'], 'output_limit')
        self.assertFalse(result.receipt['capture_complete'])
        self.assertTrue(result.receipt['effects_uncertain'])
        self.assertTrue(result.receipt['termination']['dispatched'])

    def test_missing_marker_timeout_retains_uncertain_failure(self):
        result = self.run_child('import time;time.sleep(2)', plan(timeout=.05,
            actions=[{'kind': 'write', 'data': b'x', 'after': after('stdout', b'absent')}]))
        self.assertEqual(result.receipt['error'], 'deadline')
        self.assertEqual(result.receipt['actions'], [])

    def test_early_output_eof_does_not_ack_unperformed_action(self):
        result = self.run_child('print("other")', plan(actions=[
            {'kind': 'write', 'data': b'x', 'after': after('stdout', b'never')}]))
        self.assertEqual(result.receipt['error'], 'output_closed_before_actions_complete')
        self.assertFalse(result.receipt['capture_complete'])

    def test_pty_raw_input_resize_and_client_restoration(self):
        script = '''import os,sys,termios,tty,fcntl,struct,signal,time
fd=0
original=termios.tcgetattr(fd)
assert os.isatty(fd)
assert struct.unpack('HHHH',fcntl.ioctl(fd,termios.TIOCGWINSZ,b'\\0'*8))[:2]==(24,80)
def resized(a,b):
 assert struct.unpack('HHHH',fcntl.ioctl(fd,termios.TIOCGWINSZ,b'\\0'*8))[:2]==(40,120)
 os.write(1,b'SIZED')
signal.signal(signal.SIGWINCH,resized)
try:
 tty.setraw(fd)
 os.write(1,b'READY')
 data=os.read(fd,3)
 assert data==b'abc'
 os.write(1,b'DATA')
 while True:
  if os.read(fd,1)==b'q':break
finally:termios.tcsetattr(fd,termios.TCSANOW,original)
os.write(2,b'DONE')
'''
        selected = plan('pty', [
            {'kind': 'write', 'data': b'abc', 'after': after('tty', b'READY')},
            {'kind': 'resize', 'rows': 40, 'cols': 120, 'after': after('tty', b'DATA')},
            {'kind': 'write', 'data': b'q', 'after': after('tty', b'SIZED')}])
        result = self.run_child(script, selected)
        self.assertEqual(result.stdout, b'READYDATASIZEDDONE')
        self.assertEqual(result.stderr, b'')
        self.assertEqual(result.returncode, 0)
        self.assertTrue(result.receipt['merged_tty'])
        self.assertTrue(result.receipt['capture_complete'])
        terminal = result.receipt['terminal']
        self.assertEqual(terminal['initial_attributes'], terminal['child_exited_termios'])
        self.assertTrue(terminal['restored_verified'])
        self.assertFalse(terminal['repaired_by_harness'])

    def test_harness_terminal_repair_never_manufactures_client_success(self):
        result = self.run_child('import tty;tty.setraw(0);print("raw")', plan('pty'))
        terminal = result.receipt['terminal']
        self.assertFalse(terminal['client_restored_attributes'])
        self.assertTrue(terminal['repaired_by_harness'])
        self.assertEqual(terminal['restore_attempts'], 1)
        # Darwin can retain PENDIN after the slave closes. Do not relax exact
        # restoration or claim repair succeeded merely because tcsetattr did.
        self.assertEqual(terminal['restored_verified'],
                         terminal['restored_attributes'] == terminal['initial_attributes'])
        if not terminal['restored_verified']:
            self.assertEqual(result.receipt['cleanup_error'], 'CaptureError')
        self.assertFalse(result.receipt['capture_complete'])
        self.assertEqual(result.receipt['error'], 'child_terminal_not_restored')

    def test_invalid_plans_never_spawn(self):
        bad = []
        for key, value in [('mode', 'other'), ('timeout_seconds', True), ('timeout_seconds', 121),
                           ('timeout_seconds', float('nan')), ('input_limit', True),
                           ('output_limit', io.MAX_BYTES + 1), ('actions', [{}] * 33)]:
            p = plan(); p[key] = value; bad.append(p)
        for actions in ([{'kind': 'close_stdin'}] * 2,
                        [{'kind': 'close_stdin'}, {'kind': 'write', 'data': b'x'}],
                        [{'kind': 'signal', 'name': 'SIGKILL'}],
                        [{'kind': 'resize', 'rows': 40, 'cols': 120}],
                        [{'kind': 'write', 'data': b'x', 'after': after('tty', b'a')}],
                        [{'kind': 'write', 'data': b''}]):
            bad.append(plan(actions=actions))
        bad.append(plan('pty', [{'kind': 'close_stdin'}]))
        with patch.object(io.subprocess, 'Popen') as spawn:
            for p in bad:
                with self.subTest(plan=p), self.assertRaises(io.CaptureError):
                    io.capture(['/fake'], executable='/fake', cwd='/tmp', env={}, plan=p)
            spawn.assert_not_called()

    def test_reaped_nonzero_child_never_signaled(self):
        process = Mock(pid=123, returncode=37)
        with patch.object(io.os, 'killpg') as kill, patch.object(io.os, 'getpgid') as pgid:
            io.terminate_owned(process, 123)
            kill.assert_not_called(); pgid.assert_not_called(); process.wait.assert_not_called()

    def test_identity_drift_or_missing_identity_refuses_signal(self):
        process = Mock(pid=123, returncode=None)
        for group, session in ((124, 123), (123, 124)):
            with patch.object(io.os, 'getpgid', return_value=group), \
                 patch.object(io.os, 'getsid', return_value=session), patch.object(io.os, 'killpg') as kill:
                with self.assertRaises(io.CaptureError): io.terminate_owned(process, 123)
                kill.assert_not_called(); process.wait.assert_not_called()

    def test_spawn_failure_is_recorded_without_any_signal(self):
        with patch.object(io.subprocess, 'Popen', side_effect=OSError('not echoed')), \
             patch.object(io.os, 'killpg') as kill:
            result = io.capture(['/fake'], executable='/fake', cwd='/tmp', env={}, plan=plan())
        self.assertEqual(result.receipt['error'], 'OSError')
        self.assertIsNone(result.pending_process)
        self.assertNotIn('not echoed', json.dumps(result.receipt))
        kill.assert_not_called()

    def test_signal_denial_retains_exact_handle_and_original_failure(self):
        with tempfile.TemporaryDirectory(prefix='vz-interactive-pending-') as root:
            with patch.object(io, 'signal_owned', side_effect=PermissionError('private diagnostic')):
                result = io.capture([sys.executable, '-c', 'import time;time.sleep(.15)'],
                    executable=sys.executable, cwd=root, env={}, plan=plan(timeout=.04))
            self.assertIsNotNone(result.pending_process)
            self.assertEqual(result.receipt['error'], 'deadline')
            self.assertEqual(result.receipt['cleanup_error'], 'PermissionError')
            self.assertFalse(result.receipt['termination']['dispatched'])
            self.assertFalse(result.receipt['owned_process_reaped'])
            self.assertNotIn('private diagnostic', json.dumps(result.receipt))
            # Explicit caller disposition: this finite child exits naturally;
            # wait only reaps our same retained direct child, never signals it.
            result.pending_process.wait(timeout=2)
            self.assertEqual(result.pending_process.returncode, 0)

    def test_live_group_with_pipe_holding_child_timeout_is_reaped(self):
        result = self.run_child('import os,time;pid=os.fork();'
            'os.write(1,b"owned-pipe-holder\\n");time.sleep(2)', plan(timeout=.1))
        self.assertEqual(result.returncode, -signal.SIGKILL)
        self.assertEqual(result.receipt['error'], 'deadline')
        self.assertTrue(result.receipt['termination']['dispatched'])

    def test_early_eof_failure_reaps_without_signaling_reused_pid(self):
        with patch.object(io.os, 'killpg') as kill:
            result = self.run_child('print("other")', plan(actions=[
                {'kind': 'write', 'data': b'x', 'after': after('stdout', b'never')}]))
        self.assertEqual(result.returncode, 0)
        self.assertFalse(result.receipt['capture_complete'])
        kill.assert_not_called()


if __name__ == '__main__':
    unittest.main()
