"""Pure protocol and temporary-copy fixture adversaries; no runtime clients."""
import copy
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_container_fixture as fixture

TOKEN = 'vzio-' + 'a' * 24


def record(kind, **fields):
    return {'schema_version': 1, 'type': kind, 'token': TOKEN, **fields}


def lines(rows, newline=b'\n'):
    return b''.join(fixture.encode(row) + newline for row in rows)


def tty_rows(mode='exit'):
    result = [record('tty_ready', isatty=[True]*3, rows=24, cols=80)]
    return result + ([record('tty_resized', rows=40, cols=120), record('tty_size', rows=40, cols=120),
                      record('tty_done', exit_code=37)] if mode == 'exit'
                     else [record('observed_signal', signal='SIGINT', exit_code=130)])


def identity(uid=0, pid=13):
    namespaces = {key: key+':[123]' for key in fixture.NAMESPACES}
    return record('exec_identity', uid=uid, gid=uid, cwd='/workspace', pid=pid, pid1=1,
                  root_marker='vz-container-io-root-v1\n', namespaces=namespaces,
                  pid1_namespaces=copy.deepcopy(namespaces))


def exec_bytes(row):
    return lines([row]) + fixture.marker(TOKEN, 'exec-stdout'), fixture.marker(TOKEN, 'exec-stderr')


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()/'fixture'
        shutil.copytree(fixture.FIXTURE, self.root)

    def test_exact_frozen_fixture_and_contract(self):
        self.assertEqual(fixture.fixture_contract(self.root), json.loads((self.root/'contract.json').read_bytes()))
        self.assertEqual(fixture.fixture_contract()['binary_input']['sha256'], fixture.INPUT_SHA256)

    def test_each_source_byte_change_is_rejected(self):
        for name in fixture.FILES:
            path = self.root/name; original = path.read_bytes()
            path.write_bytes(original+b'\n')
            with self.subTest(name=name), self.assertRaises(ValueError): fixture.fixture_contract(self.root)
            path.write_bytes(original)

    def test_extra_directory_missing_file_and_wrong_mode_rejected(self):
        extra = self.root/'extra'; extra.mkdir()
        with self.assertRaises(ValueError): fixture.fixture_contract(self.root)
        extra.rmdir()
        path = self.root/'probe.py'; path.chmod(0o600)
        with self.assertRaises(ValueError): fixture.fixture_contract(self.root)
        path.chmod(0o644); path.unlink()
        with self.assertRaises(ValueError): fixture.fixture_contract(self.root)

    def test_symlink_hardlink_and_fifo_rejected_before_blocking_open(self):
        path = self.root/'probe.py'; path.unlink()
        path.symlink_to(fixture.FIXTURE/'probe.py')
        with self.assertRaises(OSError): fixture.fixture_contract(self.root)
        path.unlink(); os.link(self.root/'README.md', path)
        with self.assertRaises(ValueError): fixture.fixture_contract(self.root)
        path.unlink(); os.mkfifo(path, 0o644)
        with self.assertRaises(ValueError): fixture.fixture_contract(self.root)

    def test_replaced_source_during_admission_fails_snapshot(self):
        original = fixture.regular
        count = 0
        def read(path):
            nonlocal count
            result = original(path); count += 1
            if count == 5:
                selected = self.root/'Dockerfile'
                selected.write_bytes(selected.read_bytes()+b'\n')
            return result
        with patch.object(fixture, 'regular', side_effect=read), self.assertRaises(ValueError):
            fixture.fixture_contract(self.root)

    def test_directory_redirect_during_admission_rejected_even_with_same_files(self):
        original = fixture.regular
        changed = False
        def read(path):
            nonlocal changed
            result = original(path)
            if not changed:
                changed = True
                moved = self.root.with_name('moved')
                self.root.rename(moved)
                self.root.symlink_to(moved)
            return result
        with patch.object(fixture, 'regular', side_effect=read), self.assertRaises(ValueError):
            fixture.fixture_contract(self.root)


class ProtocolTests(unittest.TestCase):
    def test_exact_binary_stream_and_transport_scope(self):
        stdout = fixture.marker(TOKEN, 'stdout-begin')+fixture.INPUT+b'\n'+fixture.marker(TOKEN, 'stdout-end')
        stderr = fixture.marker(TOKEN, 'stderr-begin')+fixture.marker(TOKEN, 'stderr-end')
        proof = fixture.validate_stream(stdout, stderr, 37, TOKEN)
        self.assertEqual(proof['input_bytes'], 65792)
        self.assertTrue(proof['host_eof_timing_and_transport_proof_required'])
        for out, err, code in ((stdout[:-1], stderr, 37), (stdout, stderr[:-1], 37),
                               (stdout.replace(b'\x00', b'\x01'), stderr, 37),
                               (stdout, stderr+b'warning\n', 37), (stdout, stderr, 0),
                               (bytearray(stdout), stderr, 37)):
            with self.assertRaises(ValueError): fixture.validate_stream(out, err, code, TOKEN)

    def test_exact_tty_resize_and_sigint_record_not_numeric_signal_claim(self):
        proof = fixture.validate_tty(lines(tty_rows(), b'\r\n'), 37, TOKEN)
        self.assertFalse(proof['signal_delivery_certified'])
        proof = fixture.validate_tty(lines(tty_rows('sigint'), b'\r\n'), 130, TOKEN, mode='sigint')
        self.assertFalse(proof['signal_delivery_certified'])
        with self.assertRaises(ValueError): fixture.validate_tty(lines(tty_rows(), b'\r\n'), 130, TOKEN, mode='sigint')
        with self.assertRaises(ValueError): fixture.validate_tty(b'', 130, TOKEN, mode='sigint')
        with self.assertRaises(ValueError): fixture.validate_tty(lines(tty_rows()), 37, TOKEN)
        fixture.validate_tty(lines(tty_rows()), 37, TOKEN, newline=b'\n')

    def test_tty_wrong_dimensions_order_boolean_types_and_foreign_output(self):
        for change in ('initial', 'resized', 'query-size', 'ack-bool', 'duplicate-ack', 'ack-order',
                       'bool', 'order', 'extra', 'missing', 'token'):
            rows = tty_rows()
            if change == 'initial': rows[0]['rows'] = 25
            elif change == 'resized': rows[1]['cols'] = 121
            elif change == 'query-size': rows[2]['cols'] = 121
            elif change == 'ack-bool': rows[1]['rows'] = True
            elif change == 'duplicate-ack': rows.insert(2, rows[1])
            elif change == 'ack-order': rows[1], rows[2] = rows[2], rows[1]
            elif change == 'bool': rows[0]['isatty'] = [1, 1, 1]
            elif change == 'order': rows.reverse()
            elif change == 'extra': rows.append(rows[-1])
            elif change == 'missing': rows.pop(1)
            elif change == 'token': rows[0]['token'] = 'vzio-'+'b'*24
            with self.subTest(change=change), self.assertRaises(ValueError):
                fixture.validate_tty(lines(rows, b'\r\n'), 37, TOKEN)

    def test_noncanonical_duplicate_malformed_or_unbounded_records(self):
        row = record('example')
        for raw in (fixture.encode(row), json.dumps(row, indent=2).encode()+b'\n',
                    b'{"schema_version":1,"schema_version":1,"token":"'+TOKEN.encode()+b'","type":"example"}\n',
                    lines([row]).replace(b'example', b'\\u0065xample'), b'NaN\n',
                    b'[]\n', b'null\n', b'\xff\n', b'\n', b'x'*65536+b'\n',
                    lines([row]*33)):
            with self.subTest(raw=raw[:70]), self.assertRaises(ValueError): fixture.records(raw, TOKEN)
        with self.assertRaises(ValueError): fixture.records(lines([row]), TOKEN+'\n')

    def test_exec_pair_exact_root_nonroot_namespaces_and_denial(self):
        root, nonroot = identity(), identity(10001, 14)
        for name in fixture.NAMESPACES:
            nonroot['pid1_namespaces'][name] = {'error': 'permission_denied', 'errno': 13}
        parsed = [fixture.parse_exec(*exec_bytes(row), 37, TOKEN) for row in (root, nonroot)]
        result = fixture.validate_exec_pair(*parsed, TOKEN)
        self.assertTrue(result['same_container_incarnation_and_host_exec_flags_proof_required'])
        nonroot['pid1_namespaces']['mnt'] = root['pid1_namespaces']['mnt']
        fixture.validate_exec_pair(root, nonroot, TOKEN)

    def test_exec_identity_uid_gid_root_and_namespace_drift(self):
        for change in ('uid', 'gid', 'cwd', 'marker', 'pid', 'pid1', 'self', 'root-pid1', 'nonroot-pid1', 'missing', 'bool'):
            root, nonroot = identity(), identity(10001, 14)
            if change == 'uid': nonroot['uid'] = 65534
            elif change == 'gid': nonroot['gid'] = 0
            elif change == 'cwd': nonroot['cwd'] = '/'
            elif change == 'marker': nonroot['root_marker'] = 'foreign'
            elif change == 'pid': nonroot['pid'] = root['pid']
            elif change == 'pid1': nonroot['pid1'] = 2
            elif change == 'self': nonroot['namespaces']['mnt'] = 'mnt:[999]'
            elif change == 'root-pid1': root['pid1_namespaces']['mnt'] = 'mnt:[999]'
            elif change == 'nonroot-pid1': nonroot['pid1_namespaces']['mnt'] = 'mnt:[999]'
            elif change == 'missing': del nonroot['namespaces']['net']
            elif change == 'bool': root['uid'] = False
            with self.subTest(change=change), self.assertRaises(ValueError): fixture.validate_exec_pair(root, nonroot, TOKEN)

    def test_namespace_denial_only_nonroot_pid1_exact_errno(self):
        for uid, field, error in ((0, 'pid1_namespaces', {'error': 'permission_denied', 'errno': 13}),
                                  (10001, 'namespaces', {'error': 'permission_denied', 'errno': 13}),
                                  (10001, 'pid1_namespaces', {'error': 'permission_denied', 'errno': True}),
                                  (10001, 'pid1_namespaces', {'error': 'permission_denied', 'errno': 2}),
                                  (10001, 'pid1_namespaces', {'error': 'missing', 'errno': 13})):
            row = identity(uid); row[field]['mnt'] = error
            with self.assertRaises(ValueError): fixture.parse_exec(*exec_bytes(row), 37, TOKEN)

    def test_exec_streams_exit_and_duplicate_identity_are_strict(self):
        out, err = exec_bytes(identity())
        for stdout, stderr, code in ((out, err, 0), (out, err+b'extra', 37),
                                     (out+out, err, 37), (out[:-1], err, 37)):
            with self.assertRaises(ValueError): fixture.parse_exec(stdout, stderr, code, TOKEN)

    def test_health_state_and_probe_operational_error_not_unhealthy(self):
        for state, code in (('starting', 1), ('healthy', 0), ('unhealthy', 1)):
            raw = lines([record('health_state', state=state)])
            fixture.validate_health_state(raw, TOKEN, state)
            fixture.validate_health_probe(b'', b'', code, state=state)
            with self.assertRaises(ValueError): fixture.validate_health_probe(b'', b'', 70, state=state)
            with self.assertRaises(ValueError): fixture.validate_health_probe(b'', b'contract rejected\n', code, state=state)
        with self.assertRaises(ValueError): fixture.validate_health_probe(b'', b'', False, state='healthy')
        with self.assertRaises(ValueError): fixture.validate_health_state(lines([record('health_state', state='healthy', extra=True)]), TOKEN, 'healthy')

    def test_service_initial_health_changes_and_real_signal_record(self):
        ready = record('service_ready', pid=1, health='starting', output='stdout')
        err = lines([{**ready, 'output': 'stderr'}])
        fixture.validate_service(lines([ready]), err, TOKEN)
        rows = [ready, record('health_changed', healthy=True, signal='SIGUSR1'),
                record('health_changed', healthy=False, signal='SIGUSR2'),
                record('observed_signal', signal='SIGTERM', exit_code=143)]
        result = fixture.validate_service(lines(rows), err, TOKEN, signals=('SIGUSR1', 'SIGUSR2', 'SIGTERM'), exit_code=143)
        self.assertFalse(result['signal_delivery_certified'])
        for stdout, stderr, signals, code in ((lines([ready]), err, (), 143),
                (lines(rows), err, ('SIGTERM',), 143), (lines(rows), err, ('SIGUSR1', 'SIGUSR2', 'SIGTERM'), 137),
                (lines(rows), err+b'warning\n', ('SIGUSR1', 'SIGUSR2', 'SIGTERM'), 143),
                (lines([{**ready, 'pid': 2}]), err, (), None)):
            with self.assertRaises(ValueError): fixture.validate_service(stdout, stderr, TOKEN, signals=signals, exit_code=code)


if __name__ == '__main__':
    unittest.main()
