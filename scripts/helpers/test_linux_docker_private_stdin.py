"""Transport adversaries plus two harmless Python pipe children; no Docker/VM."""
import base64
import copy
import hashlib
import json
import os
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import linux_docker_private_stdin as private

SECRET = b'public-unit-private-credential-619c9a\n'
ACK = b'Login Succeeded\n'
ARGV = ['docker', '--config', '/owned/machine-client', 'login', '--password-stdin', 'registry.invalid']
ENV = {'PATH': '/owned/bin', 'LC_ALL': 'C'}


def stamp(n):
    return {'unix_ns': 1000 + n, 'monotonic_ns': 2000 + n}


def captured():
    digest = lambda data: hashlib.sha256(data).hexdigest()
    row = {'argv': list(ARGV), 'executable': '/owned/docker', 'cwd': '/owned/evidence', 'environment': dict(ENV),
        'mode': 'pipes', 'merged_tty': False, 'terminal': None, 'started': stamp(0), 'completed': stamp(5),
        'capture_complete': True, 'effects_uncertain': False, 'owned_process_reaped': True,
        'owned_direct_child': True, 'pid': 123, 'process_group': 123, 'session_id': 123,
        'error': None, 'cleanup_error': None, 'termination': None, 'returncode': 0, 'stdin_eof_count': 1,
        'input_limit': len(SECRET), 'output_limit_each': private.MAX_OUTPUT, 'timeout_seconds': 30,
        'planned_action_count': 2, 'actions': [
            {'index': 0, 'kind': 'write', 'trigger': {'kind': 'immediate'}, 'complete': True,
             'input_size': len(SECRET), 'written_bytes': len(SECRET), 'input_sha256': digest(SECRET),
             'triggered': stamp(1), 'completed': stamp(2)},
            {'index': 1, 'kind': 'close_stdin', 'trigger': {'kind': 'immediate'}, 'complete': True,
             'triggered': stamp(3), 'completed': stamp(4)}],
        'outputs': {name: {'size': len(data), 'sha256': digest(data)}
                    for name, data in (('stdout', ACK), ('stderr', b''), ('tty', b''))}}
    return SimpleNamespace(stdout=ACK, stderr=b'', returncode=0, receipt=row, pending_process=None)


class PrivateStdinTests(unittest.TestCase):
    def owner(self, **options):
        selected = {'executable': '/owned/docker', 'cwd': '/owned/evidence', 'env': dict(ENV),
                    'private_input': SECRET, 'expected_stdout': ACK, 'expected_stderr': b''}
        selected.update(options)
        return private.Capture(list(ARGV), **selected)

    def public_only(self, result):
        raw = json.dumps(result.receipt, sort_keys=True).encode() + result.stdout + result.stderr + repr(result).encode()
        for secret in (SECRET, SECRET.rstrip(b'\n')):
            for forbidden in (secret, base64.b64encode(secret), secret.hex().encode(), hashlib.sha256(secret).hexdigest().encode()):
                self.assertNotIn(forbidden, raw)
        self.assertNotIn('actions', result.receipt)
        self.assertFalse(result.receipt['private_input_hash_published'])
        self.assertFalse(result.receipt['private_plan_published'])

    def test_exact_public_ack_private_plan_stays_memory_only(self):
        owner = self.owner()
        self.assertTrue(owner.receipt['effects_uncertain'])
        self.assertNotIn(SECRET.decode(), repr(owner))
        with patch.object(private.transport, 'capture', return_value=captured()) as execute:
            result = owner.run()
        self.assertEqual(execute.call_args.args, (ARGV,))
        args = execute.call_args.kwargs
        self.assertEqual(args['executable'], '/owned/docker')
        self.assertEqual(args['cwd'], '/owned/evidence')
        self.assertEqual(args['env'], ENV)
        self.assertEqual(args['plan']['actions'], [{'kind': 'write', 'data': SECRET}, {'kind': 'close_stdin'}])
        self.assertFalse(result.receipt['effects_uncertain'])
        self.assertTrue(result.receipt['acknowledged'])
        self.assertTrue(result.receipt['stdin_write_complete'])
        self.assertEqual(result.receipt['stdin_eof_count'], 1)
        self.assertEqual((result.stdout, result.stderr, result.returncode), (ACK, b'', 0))
        self.public_only(result)
        with self.assertRaises(private.PrivateStdinError), patch.object(private.transport, 'capture', side_effect=AssertionError('retry')):
            owner.run()

    def test_private_receipt_copy_and_supplied_environment_are_not_mutable_aliases(self):
        environment = dict(ENV)
        owner = self.owner(env=environment)
        environment['LC_ALL'] = 'foreign'
        receipt = owner.receipt
        receipt['argv'].append(SECRET.decode())
        receipt['effects_uncertain'] = False
        self.assertTrue(owner.receipt['effects_uncertain'])
        with patch.object(private.transport, 'capture', return_value=captured()) as execute:
            result = owner.run()
        self.assertEqual(execute.call_args.kwargs['env'], ENV)
        self.public_only(result)

    def test_every_unexpected_output_withheld_including_unrecognized_encodings(self):
        for stream in ('stdout', 'stderr'):
            for data in (SECRET, base64.b64encode(SECRET), SECRET.hex().encode(),
                         b'encoded-using-an-unrecognized-protocol', ACK + b'extra'):
                value = captured()
                setattr(value, stream, data)
                with self.subTest(stream=stream, data=data), patch.object(private.transport, 'capture', return_value=value):
                    result = self.owner().run()
                self.assertEqual((result.stdout, result.stderr), (b'', b''))
                self.assertTrue(result.receipt['effects_uncertain'])
                self.assertTrue(result.receipt['unexpected_output_withheld'])
                self.public_only(result)

    def test_private_public_contract_rejected_before_dispatch(self):
        for value in (SECRET.rstrip(b'\n'), base64.b64encode(SECRET), SECRET.hex().encode()):
            for field in ('expected_stdout', 'expected_stderr', 'env', 'cwd', 'executable'):
                changed = {field: value if field.startswith('expected_') else
                    {'PRIVATE': value.decode()} if field == 'env' else '/owned/' + value.decode()}
                with self.subTest(field=field), patch.object(private.transport, 'capture', side_effect=AssertionError('dispatch')):
                    with self.assertRaisesRegex(private.PrivateStdinError, 'private_value_in_public_contract'):
                        self.owner(**changed)
            with self.assertRaises(private.PrivateStdinError):
                private.Capture(ARGV + [value.decode()], executable='/owned/docker', cwd='/owned', env={},
                                private_input=SECRET, expected_stdout=ACK, expected_stderr=b'')

    def test_partial_write_eof_clock_identity_limits_and_output_binding_reject(self):
        mutations = [lambda r: r['actions'][0].update(complete=False),
            lambda r: r['actions'][0].update(written_bytes=1), lambda r: r['actions'][0].update(index=False),
            lambda r: r['actions'][0].update(input_sha256='0' * 64), lambda r: r['actions'].pop(),
            lambda r: r['actions'][1].update(complete=False), lambda r: r.update(stdin_eof_count=2),
            lambda r: r.update(stdin_eof_count=True), lambda r: r['actions'][1].update(triggered=stamp(-10)),
            lambda r: r.update(argv=['foreign']), lambda r: r.update(environment={'FOREIGN': 'yes'}),
            lambda r: r.update(executable='/foreign'), lambda r: r.update(cwd='/foreign'),
            lambda r: r.update(process_group=124), lambda r: r.update(input_limit=1),
            lambda r: r.update(output_limit_each=1), lambda r: r.update(timeout_seconds=121),
            lambda r: r.update(planned_action_count=True), lambda r: r.update(returncode=False),
            lambda r: r['outputs']['stdout'].update(sha256='0' * 64), lambda r: r.update(owned_process_reaped=False),
            lambda r: r.update(capture_complete=False), lambda r: r.update(effects_uncertain=True)]
        for mutate in mutations:
            value = captured()
            mutate(value.receipt)
            with patch.object(private.transport, 'capture', return_value=value):
                result = self.owner().run()
            self.assertTrue(result.receipt['effects_uncertain'])
            self.assertFalse(result.receipt['acknowledged'])
            self.assertEqual(result.stdout, b'')
            self.public_only(result)

    def test_pending_handle_and_sticky_uncertainty_survive_redacted_failure(self):
        value = captured()
        pending = SimpleNamespace(returncode=None, pid=123)
        value.pending_process = pending
        value.returncode = None
        value.receipt.update(owned_process_reaped=False, capture_complete=False, effects_uncertain=True,
                             error=SECRET.decode(), cleanup_error=SECRET.hex(),
                             termination={'signal': 'SIGKILL', 'private': SECRET.decode()})
        owner = self.owner()
        with patch.object(private.transport, 'capture', return_value=value):
            result = owner.run()
        self.assertIs(owner.pending_process, pending)
        self.assertIs(result.pending_process, pending)
        self.assertTrue(result.receipt['process_ownership_unresolved'])
        self.assertTrue(result.receipt['recovery_attempted'])
        self.public_only(result)

    def test_exception_message_output_cause_and_private_attributes_never_exported(self):
        error = subprocess.TimeoutExpired([SECRET.decode()], 30, output=SECRET, stderr=base64.b64encode(SECRET))
        error.__cause__ = ValueError(SECRET.hex())
        error.pending_process = SimpleNamespace(pid=123, returncode=None)
        with patch.object(private.transport, 'capture', side_effect=error):
            result = self.owner().run()
        self.assertIs(result.pending_process, error.pending_process)
        self.assertTrue(result.receipt['effects_uncertain'])
        self.assertEqual(result.receipt['error'], 'transport_exception')
        self.assertIsNone(result.returncode)
        self.public_only(result)

    def test_expected_nonzero_requires_complete_exact_ack_no_signal_substitution(self):
        for code in (1, 37, 255):
            value = captured()
            value.returncode = value.receipt['returncode'] = code
            with patch.object(private.transport, 'capture', return_value=value):
                result = self.owner(expected_exit=code).run()
            self.assertTrue(result.receipt['acknowledged'])
        for code in (-9, 1, None, True):
            value = captured()
            value.returncode = value.receipt['returncode'] = code
            with patch.object(private.transport, 'capture', return_value=value):
                self.assertTrue(self.owner().run().receipt['effects_uncertain'])

    def test_invalid_limits_types_and_paths_fail_before_capture(self):
        for changed in ({'private_input': b''}, {'private_input': b'x' * (private.MAX_INPUT + 1)},
                        {'private_input': 'text'}, {'timeout_seconds': True}, {'timeout_seconds': float('nan')},
                        {'timeout_seconds': 121}, {'output_limit': private.MAX_OUTPUT + 1},
                        {'output_limit': False}, {'expected_exit': True}, {'expected_exit': -9},
                        {'expected_stdout': 'text'}, {'cwd': 'relative'}, {'env': {'BAD=KEY': 'value'}}):
            with self.subTest(changed=changed), patch.object(private.transport, 'capture', side_effect=AssertionError('dispatch')):
                with self.assertRaises(private.PrivateStdinError):
                    self.owner(**changed)

    def test_native_pipe_fixed_public_acknowledgment(self):
        program = 'import sys; sys.stdin.buffer.read(); sys.stdout.buffer.write(b"Login Succeeded\\n")'
        executable = os.path.realpath(sys.executable)
        owner = private.Capture([executable, '-I', '-B', '-c', program], executable=executable,
            cwd=os.getcwd(), env={'LC_ALL': 'C'}, private_input=SECRET,
            expected_stdout=ACK, expected_stderr=b'', timeout_seconds=5)
        result = owner.run()
        self.assertTrue(result.receipt['acknowledged'], result.receipt)
        self.assertTrue(result.receipt['owned_process_reaped'])
        self.assertIsNone(result.pending_process)
        self.assertEqual(result.returncode, 0)
        self.public_only(result)

    def test_native_pipe_encoded_secret_is_wholly_withheld(self):
        program = 'import sys,base64; sys.stdout.buffer.write(base64.b64encode(sys.stdin.buffer.read()))'
        executable = os.path.realpath(sys.executable)
        owner = private.Capture([executable, '-I', '-B', '-c', program], executable=executable,
            cwd=os.getcwd(), env={'LC_ALL': 'C'}, private_input=SECRET,
            expected_stdout=ACK, expected_stderr=b'', timeout_seconds=5)
        result = owner.run()
        self.assertFalse(result.receipt['acknowledged'])
        self.assertTrue(result.receipt['effects_uncertain'])
        self.assertTrue(result.receipt['owned_process_reaped'])
        self.assertIsNone(result.pending_process)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b'')
        self.public_only(result)

    def test_cancellation_or_hostile_exception_accessor_stays_uncertain(self):
        class HostileError(Exception):
            @property
            def pending_process(self):
                raise RuntimeError(SECRET.decode())
        for error in (KeyboardInterrupt(SECRET.decode()), HostileError(SECRET.hex())):
            with patch.object(private.transport, 'capture', side_effect=error):
                result = self.owner().run()
            self.assertTrue(result.receipt['effects_uncertain'])
            self.assertTrue(result.receipt['process_ownership_unresolved'])
            self.assertFalse(result.receipt['owned_process_reaped'])
            self.public_only(result)


if __name__ == '__main__':
    unittest.main()
