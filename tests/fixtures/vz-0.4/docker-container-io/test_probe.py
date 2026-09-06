"""Finite local Python/PTY tests only; no Docker, VM, network or agent."""
import copy
import errno
import fcntl
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import select
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import termios
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('container_io_probe', ROOT / 'probe.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)
TOKEN = 'vzio-' + 'a' * 24
INPUT = bytes(range(256)) * 257


class ProbeTests(unittest.TestCase):
    def run_probe(self, *args, data=b''):
        return subprocess.run([sys.executable, '-B', str(ROOT / 'probe.py'), *args],
                              input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)

    def test_binary_stream_preserves_all_bytes_and_delayed_eof_trailer(self):
        began = time.monotonic()
        result = self.run_probe('stream', TOKEN, data=INPUT)
        self.assertEqual(result.returncode, 37)
        self.assertEqual(result.stdout, probe.marker(TOKEN, 'stdout-begin') + INPUT + b'\n' + probe.marker(TOKEN, 'stdout-end'))
        self.assertEqual(result.stderr, probe.marker(TOKEN, 'stderr-begin') + probe.marker(TOKEN, 'stderr-end'))
        self.assertGreaterEqual(time.monotonic() - began, .1)

    def test_empty_stream_and_oversized_stream(self):
        result = self.run_probe('stream', TOKEN)
        self.assertEqual(result.returncode, 37)
        self.assertEqual(result.stdout, probe.marker(TOKEN, 'stdout-begin') + b'\n' + probe.marker(TOKEN, 'stdout-end'))
        result = self.run_probe('stream', TOKEN, data=b'x' * (probe.MAX_INPUT + 1))
        self.assertEqual(result.returncode, 70)
        self.assertEqual(result.stdout, b'')
        self.assertTrue(result.stderr.endswith(b'VZ_CONTAINER_IO_CONTRACT_REJECTED\n'))

    def test_no_stream_stdout_before_actual_eof(self):
        process = subprocess.Popen([sys.executable, '-B', str(ROOT / 'probe.py'), 'stream', TOKEN],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            process.stdin.write(b'held-input'); process.stdin.flush()
            self.assertFalse(select.select([process.stdout], [], [], .15)[0])
            process.stdin.close(); process.stdin = None
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 37)
            self.assertIn(b'held-input\n', stdout)
            self.assertTrue(stderr.endswith(probe.marker(TOKEN, 'stderr-end')))
        finally:
            if process.poll() is None:
                process.kill(); process.wait(timeout=3)
            for pipe in (process.stdin, process.stdout, process.stderr):
                if pipe is not None: pipe.close()

    def test_restricted_exit_codes_empty_streams(self):
        for code in (0, 37, 130, 137, 143):
            result = self.run_probe('exit', str(code))
            self.assertEqual((result.returncode, result.stdout, result.stderr), (code, b'', b''))
        for value in ('-1', '256', '00', '1', 'SIGTERM'):
            self.assertEqual(self.run_probe('exit', value).returncode, 70)

    def test_invalid_tokens_modes_and_argument_counts_fail(self):
        for args in ((), ('stream',), ('stream', TOKEN, 'extra'), ('unknown', TOKEN),
                     ('stream', TOKEN+'\n'), ('stream', '../foreign'), ('stream', 'vzio-'+'a'*25)):
            result = self.run_probe(*args)
            self.assertEqual((result.returncode, result.stdout, result.stderr),
                             (70, b'', b'VZ_CONTAINER_IO_CONTRACT_REJECTED\n'))

    def test_tty_requires_actual_three_terminal_descriptors(self):
        self.assertEqual(self.run_probe('tty', TOKEN).returncode, 70)

    def tty_session(self, interrupt=False):
        master, slave = os.openpty()
        saved = termios.tcgetattr(slave)
        initial = copy.deepcopy(saved)
        initial[3] |= termios.ECHO | termios.ICANON | termios.ISIG
        termios.tcsetattr(slave, termios.TCSANOW, initial)
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
        process = subprocess.Popen([sys.executable, '-B', str(ROOT / 'probe.py'), 'tty', TOKEN],
                                   stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        pending = bytearray()
        def line():
            end = time.monotonic() + 3
            while b'\n' not in pending:
                remaining = end-time.monotonic()
                self.assertGreater(remaining, 0)
                self.assertTrue(select.select([master], [], [], remaining)[0])
                pending.extend(os.read(master, 4096))
            raw, _, rest = pending.partition(b'\n'); pending[:] = rest
            return json.loads(raw.rstrip(b'\r'))
        try:
            ready = line()
            self.assertEqual(ready, {'schema_version': 1, 'type': 'tty_ready', 'token': TOKEN,
                                    'isatty': [True]*3, 'rows': 24, 'cols': 80})
            current = termios.tcgetattr(slave)
            self.assertFalse(current[3] & termios.ECHO)
            self.assertEqual(current[3], initial[3] & ~termios.ECHO)
            if interrupt:
                process.send_signal(signal.SIGINT)
                self.assertEqual(line(), {'schema_version': 1, 'type': 'observed_signal', 'token': TOKEN,
                                          'signal': 'SIGINT', 'exit_code': 130})
                expected = 130
            else:
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 40, 120, 0, 0))
                os.write(master, b'size\n')
                self.assertEqual(line(), {'schema_version': 1, 'type': 'tty_size', 'token': TOKEN, 'rows': 40, 'cols': 120})
                os.write(master, b'exit\n')
                self.assertEqual(line(), {'schema_version': 1, 'type': 'tty_done', 'token': TOKEN, 'exit_code': 37})
                expected = 37
            self.assertEqual(process.wait(timeout=3), expected)
            self.assertEqual(termios.tcgetattr(slave), initial)
        finally:
            if process.poll() is None:
                process.kill(); process.wait(timeout=3)
            termios.tcsetattr(slave, termios.TCSANOW, saved)
            os.close(slave); os.close(master)

    def test_actual_owned_pty_resize_and_guest_restoration(self):
        self.tty_session()

    def test_actual_owned_sigint_and_guest_restoration(self):
        self.tty_session(interrupt=True)

    def test_exact_health_state_and_foreign_malformed_links_rejected(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(probe, 'HEALTH', Path(temporary)/'health'):
            for state in ('starting', 'healthy', 'unhealthy'):
                probe.write_health(TOKEN, state)
                self.assertEqual(probe.read_health(TOKEN), state)
                self.assertEqual(stat.S_IMODE(probe.HEALTH.stat().st_mode), 0o644)
            original = probe.HEALTH.read_bytes()
            for raw in (b'', original+b'\n', original.replace(b'"unhealthy"', b'false'),
                        original.replace(TOKEN.encode(), ('vzio-'+'b'*24).encode())):
                probe.HEALTH.write_bytes(raw)
                with self.assertRaises(ValueError): probe.read_health(TOKEN)
            probe.HEALTH.write_bytes(original); probe.HEALTH.chmod(0o666)
            with self.assertRaises(ValueError): probe.read_health(TOKEN)
            probe.HEALTH.chmod(0o644)
            linked = probe.HEALTH.with_name('linked'); os.link(probe.HEALTH, linked)
            with self.assertRaises(ValueError): probe.read_health(TOKEN)
            linked.unlink(); probe.HEALTH.unlink(); probe.HEALTH.symlink_to(linked)
            with self.assertRaises(OSError): probe.read_health(TOKEN)

    def test_health_writer_does_not_replace_foreign_token(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(probe, 'HEALTH', Path(temporary)/'health'):
            probe.write_health(TOKEN, 'healthy')
            before = probe.HEALTH.read_bytes()
            with self.assertRaises(ValueError): probe.write_health('vzio-'+'b'*24, 'unhealthy')
            self.assertEqual(probe.HEALTH.read_bytes(), before)

    def test_actual_owned_service_health_signals_and_sigterm(self):
        with tempfile.TemporaryDirectory() as temporary:
            health = Path(temporary)/'health'
            code = ("import sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);"
                    "import probe;probe.HEALTH=Path(sys.argv[2]);raise SystemExit(probe.main(['service',sys.argv[3]]))")
            process = subprocess.Popen([sys.executable, '-B', '-c', code, str(ROOT), str(health), TOKEN],
                                       stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            def line(pipe):
                self.assertTrue(select.select([pipe], [], [], 3)[0])
                return json.loads(pipe.readline(4096))
            try:
                for pipe, output in ((process.stdout, 'stdout'), (process.stderr, 'stderr')):
                    self.assertEqual(line(pipe), {'schema_version': 1, 'type': 'service_ready', 'token': TOKEN,
                                                 'pid': process.pid, 'health': 'starting', 'output': output})
                with mock.patch.object(probe, 'HEALTH', health):
                    self.assertEqual(probe.read_health(TOKEN), 'starting')
                    self.assertEqual(probe.main(['health', TOKEN]), 1)
                for number, healthy in ((signal.SIGUSR2, False), (signal.SIGUSR1, True)):
                    process.send_signal(number)
                    self.assertEqual(line(process.stdout), {'schema_version': 1, 'type': 'health_changed',
                        'token': TOKEN, 'healthy': healthy, 'signal': 'SIGUSR1' if healthy else 'SIGUSR2'})
                    with mock.patch.object(probe, 'HEALTH', health):
                        self.assertEqual(probe.read_health(TOKEN), 'healthy' if healthy else 'unhealthy')
                        self.assertEqual(probe.main(['health', TOKEN]), 0 if healthy else 1)
                process.send_signal(signal.SIGTERM)
                self.assertEqual(line(process.stdout), {'schema_version': 1, 'type': 'observed_signal',
                                                       'token': TOKEN, 'signal': 'SIGTERM', 'exit_code': 143})
                stdout, stderr = process.communicate(timeout=3)
                self.assertEqual((process.returncode, stdout, stderr), (143, b'', b''))
            finally:
                if process.poll() is None:
                    process.kill(); process.wait(timeout=3)
                process.stdout.close(); process.stderr.close()

    def test_exec_identity_and_stream_markers(self):
        stdout, stderr = io.BytesIO(), io.BytesIO()
        namespaces = {key: key+':[123]' for key in probe.NAMESPACES}
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary)/'root-marker'
            marker.write_bytes(probe.ROOT_BYTES); marker.chmod(0o644)
            self.assert_exec_identity(marker, stdout, stderr, namespaces)

    def assert_exec_identity(self, marker_path, stdout, stderr, namespaces):
        with mock.patch.object(probe, 'ROOT_MARKER', marker_path), \
             mock.patch.object(probe, 'namespace_ids', return_value=namespaces), \
             mock.patch.object(probe.os, 'getuid', return_value=10001), \
             mock.patch.object(probe.os, 'getgid', return_value=10001), \
             mock.patch.object(probe.os, 'getcwd', return_value='/workspace'), \
             mock.patch.object(probe.os, 'getpid', return_value=1234), \
             mock.patch.object(probe.sys, 'stdout', buffer=stdout), mock.patch.object(probe.sys, 'stderr', buffer=stderr):
            self.assertEqual(probe.execute(TOKEN), 37)
        identity, marker = stdout.getvalue().splitlines()
        self.assertEqual(json.loads(identity), {'schema_version': 1, 'type': 'exec_identity', 'token': TOKEN,
            'uid': 10001, 'gid': 10001, 'cwd': '/workspace', 'pid': 1234, 'pid1': 1,
            'root_marker': probe.ROOT_BYTES.decode(), 'namespaces': namespaces, 'pid1_namespaces': namespaces})
        self.assertEqual(marker+b'\n', probe.marker(TOKEN, 'exec-stdout'))
        self.assertEqual(stderr.getvalue(), probe.marker(TOKEN, 'exec-stderr'))

    def test_root_marker_malformed_or_redirected_is_not_identity_proof(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(probe, 'ROOT_MARKER', Path(temporary)/'marker'):
            probe.ROOT_MARKER.write_bytes(probe.ROOT_BYTES+b'extra'); probe.ROOT_MARKER.chmod(0o644)
            with self.assertRaises(ValueError): probe.execute(TOKEN)
            probe.ROOT_MARKER.unlink(); probe.ROOT_MARKER.symlink_to(Path(temporary)/'foreign')
            with self.assertRaises(OSError): probe.execute(TOKEN)

    def test_input_read_timeout_is_operational_failure_not_eof(self):
        with mock.patch.object(probe.select, 'select', return_value=([], [], [])), \
             mock.patch.object(probe.os, 'read') as read:
            with self.assertRaises(ValueError): probe.read_bounded(0, probe.MAX_INPUT, time.monotonic()+1)
            read.assert_not_called()

    def test_health_restart_resets_only_same_owned_token_to_starting(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(probe, 'HEALTH', Path(temporary)/'health'):
            probe.write_health(TOKEN, 'healthy')
            probe.write_health(TOKEN, 'starting')
            self.assertEqual(probe.read_health(TOKEN), 'starting')
            before = probe.HEALTH.read_bytes()
            foreign = probe.HEALTH.with_name(probe.HEALTH.name+'.next'); foreign.write_bytes(b'foreign')
            with self.assertRaises(FileExistsError): probe.write_health(TOKEN, 'healthy')
            self.assertEqual(probe.HEALTH.read_bytes(), before)
            self.assertEqual(foreign.read_bytes(), b'foreign')

    def test_namespace_permission_is_typed_only_for_nonroot_pid1(self):
        error = PermissionError(errno.EACCES, 'denied')
        with mock.patch.object(probe.os, 'readlink', side_effect=error), mock.patch.object(probe.os, 'geteuid', return_value=10001):
            self.assertEqual(probe.namespace_ids(1), {k: {'error': 'permission_denied', 'errno': 13} for k in probe.NAMESPACES})
            with self.assertRaises(ValueError): probe.namespace_ids('self')
        with mock.patch.object(probe.os, 'readlink', side_effect=error), mock.patch.object(probe.os, 'geteuid', return_value=0):
            with self.assertRaises(ValueError): probe.namespace_ids(1)
        with mock.patch.object(probe.os, 'readlink', return_value='foreign:[1]'):
            with self.assertRaises(ValueError): probe.namespace_ids('self')

    def test_contract_and_image_public_modes(self):
        contract = json.loads((ROOT/'contract.json').read_bytes())
        self.assertEqual(contract['binary_input']['sha256'], hashlib.sha256(INPUT).hexdigest())
        self.assertEqual(contract['binary_input']['size'], len(INPUT))
        self.assertEqual(contract['binary_input']['maximum_size'], probe.MAX_INPUT)
        self.assertEqual(contract['service']['lifetime_seconds'], probe.SERVICE_TIMEOUT)
        self.assertEqual(contract['stream']['timeout_seconds'], probe.IO_TIMEOUT)
        recipe = (ROOT/'Dockerfile').read_text()
        self.assertIn('ARG FIXTURE_BASE='+contract['base_reference']+'\n', recipe)
        self.assertIn('COPY --chmod=0644 probe.py /fixture/probe.py\n', recipe)
        self.assertIn('WORKDIR /workspace\n', recipe)
        self.assertNotIn('chmod -R', recipe)
        compile((ROOT/'probe.py').read_text(), 'probe.py', 'exec')


if __name__ == '__main__':
    unittest.main()
